from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Intern", "Engineer"]))
    .groupBy("job_title")
    .agg(F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"))
    .filter(F.col("analyst_count") >= 6)
    .select("job_title", "analyst_count")
)
