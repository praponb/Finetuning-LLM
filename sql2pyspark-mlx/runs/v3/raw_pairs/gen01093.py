from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "dept_id", F.ceil("salary").alias("salary_whole"))
    .orderBy(F.col("id").desc())
)
