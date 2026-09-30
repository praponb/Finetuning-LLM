from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Mia%"))
    .groupBy("is_remote", "level")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") >= 109000)
    .orderBy(F.col("level").desc())
)
