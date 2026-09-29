from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.when(F.col("budget") >= 4378000, "High").when(F.col("budget") >= 2031000, "Medium").otherwise("Low").alias("budget_tier"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

base = (
    valid
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title").isin(["Director", "Analyst"])), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("dept_name") == "HR")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_filled",
        F.when(F.col("dept_name").isin(["Finance", "Marketing"]), "Priority").otherwise("Other").alias("dept_name_group"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_filled_2"),
    )
)

result = (
    filtered
    .cube(
        F.when(F.col("budget") >= 4142000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.count_if(F.col("dept_name") == "Legal").alias("legal_events"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
    )
    .filter(F.col("engineering_budget") >= 0)
    .orderBy("budget_tier")
)
