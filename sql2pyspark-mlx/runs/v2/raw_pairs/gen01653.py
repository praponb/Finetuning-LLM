from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
        F.sum("budget").alias("total_budget"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy(F.col("total_budget").desc())
)
