from pyspark.sql import functions as F

result = (
    df_sessions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.device",
        "e.browser",
        "e.started_at",
        F.col("d.email"),
        F.col("d.city"),
        F.col("d.country").alias("customer_country"),
    )
)
