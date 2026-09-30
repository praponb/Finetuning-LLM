from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["channel", "method"], ["channel"], []],
        F.col("d.channel").alias("channel"),
        F.col("src.method").alias("method"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.amount_int")).alias("min_amount_int"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
