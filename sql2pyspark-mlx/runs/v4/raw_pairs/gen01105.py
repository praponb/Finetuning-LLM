from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "manager_id")
    .pivot("job_title", ["Analyst", "Engineer", "Director"])
    .agg(F.avg("salary").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
