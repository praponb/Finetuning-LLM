from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Hen%"))
    .groupBy("is_remote")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") >= 102000)
    .select("is_remote", "total_salary", "avg_salary")
    .orderBy("total_salary")
)
