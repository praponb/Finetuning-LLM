from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Director", "Manager"]))
    .groupBy("job_title")
    .agg(F.sum("salary").alias("total_salary"))
    .filter(F.col("total_salary") >= 494000)
    .orderBy(F.col("total_salary").desc())
)
