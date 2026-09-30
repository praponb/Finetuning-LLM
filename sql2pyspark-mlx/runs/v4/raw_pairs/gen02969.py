from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id")

filtered = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

enriched = (
    filtered
    .withColumn("prev_is_failed", F.lag("is_failed", 2).over(w_hist))
    .withColumn("order_id_avg_amount", F.avg("amount").over(w2))
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.shipping_fee"))
)

enriched_payments = (
    joined
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.status").alias("order_status"),
        F.col("b.amount").alias("order_amount"),
    )
)

totals = (
    enriched_payments
    .groupBy(F.date_trunc("year", "paid_at").alias("year_start"))
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.max("paid_at").alias("ended_at"),
        F.percentile_approx("order_id_avg_amount", 0.5).alias("order_id_avg_amount_p50"),
    )
)

result = (
    totals
    .select("year_start", "ended_at")
    .orderBy(F.col("ended_at").asc_nulls_first())
)
