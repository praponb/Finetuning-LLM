from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(F.col("dept_name") != "Finance")
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

location_summary = (
    recent_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget_filled")).otherwise(0)).alias("marketing_budget_filled"),
    )
)

result = (
    location_summary
    .filter(F.col("marketing_budget_filled") >= 64)
    .select("location", "marketing_budget_filled")
    .orderBy(F.col("marketing_budget_filled").desc())
)
