from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("cnt"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") >= 19790000)
    .drop("total_budget")
    .orderBy("total_count")
)
