from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.col("level") == "L3") & (F.col("job_title") != "Designer")) | F.col("full_name").isNotNull())
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
