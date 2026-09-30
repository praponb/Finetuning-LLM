from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "Marketing") & (F.col("dept_name") == "Finance"))
    .groupBy("location", "dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
        F.min("budget").alias("min_budget"),
    )
)
