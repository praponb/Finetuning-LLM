from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(F.col("job_title").alias("label"))
    .subtract(
        df_payments
        .filter(F.col("method").isNotNull())
        .select(F.col("method").alias("label"))
    )
    .orderBy("label")
)
