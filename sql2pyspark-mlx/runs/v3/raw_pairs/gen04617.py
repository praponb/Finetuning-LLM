from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count("budget").alias("budget_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("budget_count") < 1)
)
