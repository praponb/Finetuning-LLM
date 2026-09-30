from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("location") == "Tokyo") & ((F.col("location") == "Singapore") | (F.col("dept_name") == "Legal")))
)

location_summary = (
    base
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
    )
)

result = location_summary
