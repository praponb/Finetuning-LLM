from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
    )
    .orderBy("country", "id")
)
