from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Engineering"]))
    .select(
        "*",
        F.lower("dept_name").alias("dept_name_lower"),
        F.lower("location").alias("location_lower"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_filled"),
    )
)

result = (
    cleaned
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
    )
    .orderBy(F.col("location").desc_nulls_last())
)
