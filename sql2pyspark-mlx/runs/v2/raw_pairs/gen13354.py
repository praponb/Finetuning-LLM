from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull() & F.month("hire_date").isin([5, 6, 11]) & (F.col("salary") > 136000))
    .groupBy("job_title")
    .agg(F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"))
    .orderBy(F.col("job_title").desc())
    .limit(1)
)
