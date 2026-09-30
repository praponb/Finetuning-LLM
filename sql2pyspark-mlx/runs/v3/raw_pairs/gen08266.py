from pyspark.sql import functions as F

base = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Sales", "Finance"]) & (~F.col("location").isin(["Chiang Mai", "New York"]) | F.col("location").isin(["Berlin", "Chiang Mai", "Tokyo"])))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("unassigned")))
)

summary = (
    base
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))) / F.count("*")), 1).alias("pct_marketing"),
        F.sum("budget").alias("total_budget"),
    )
)

result = (
    summary
    .filter(F.col("total_budget") < 9218000)
    .select("location", "pct_marketing", "total_budget")
)
