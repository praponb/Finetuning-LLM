from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-26 14:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
    )
)

result = (
    base
    .groupBy(
        "method",
        F.when(F.col("status_length") >= 65, "HIGH").otherwise("LOW").alias("status_length_tier"),
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.count("*").alias("num_rows"),
        F.min("paid_at").alias("started_at"),
    )
    .orderBy("duration_min")
)
