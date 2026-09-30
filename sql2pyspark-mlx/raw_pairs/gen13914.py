from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(F.lit("employee").alias("record_type"), "job_title")
    .union(
        df_sessions
        .filter(F.col("device").isNotNull())
        .select(F.lit("session").alias("record_type"), "device")
    )
    .distinct()
)
