from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() | F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
    .orderBy(F.col("started_at_day").desc())
)
