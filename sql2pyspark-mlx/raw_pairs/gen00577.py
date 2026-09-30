from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("is_remote") & (F.col("job_title") == "Engineer") & (F.col("salary") < 86000))
    .withColumn("level_lower", F.lower("level"))
)

summary = (
    src
    .groupBy("job_title")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    summary
    .filter(F.col("max_salary") < 130000)
    .select("job_title", "max_salary")
    .orderBy("job_title")
    .limit(10)
)
