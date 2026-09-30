from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L1")
    .select("id", "manager_id", "job_title", F.length("level").alias("level_length"))
)
