from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.count("*").alias("row_count"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") > 88000)
    .select("is_remote", "row_count", "avg_salary")
    .orderBy(F.col("row_count").desc())
)
