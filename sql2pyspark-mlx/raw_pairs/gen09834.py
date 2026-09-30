from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("total_count").desc(), "dept_name")
)
