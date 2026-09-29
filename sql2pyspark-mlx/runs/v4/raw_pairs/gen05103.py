from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("is_refunded").desc(), "id")

valid = (
    df_payments
    .filter(F.col("order_id") == 39)
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

valid_payments = (
    valid
    .filter((F.col("paid_at") >= F.lit("2024-01-15 21:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "method_clean",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

filtered = (
    valid_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-12-02 01:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "paid_at",
        "status",
        "is_refunded",
        "status_prefix",
        F.date_trunc("MONTH", "paid_at").alias("paid_at_period"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select("id", "paid_at", "is_refunded", "latest_rank")
    .orderBy(F.col("latest_rank").desc())
)
