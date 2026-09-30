from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0)).alias("sales_count"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("min_budget") > 3529000)
)
