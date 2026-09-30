from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Manager", "Analyst"]))
    .groupBy("level", "is_remote")
    .agg(F.min("salary").alias("min_salary"))
    .filter(F.col("min_salary") >= 105000)
    .select("level", "is_remote", "min_salary")
)
