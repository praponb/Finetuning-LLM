from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") == "L1") & (F.length("full_name") < 12) & (F.col("job_title") == "Manager"))
    .groupBy("is_remote")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") > 131000)
)
