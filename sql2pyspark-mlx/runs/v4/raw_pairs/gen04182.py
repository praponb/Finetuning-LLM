from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("year_start").orderBy(F.col("distinct_methods").desc())

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("is_promptpay", F.when(F.col("method") == "promptpay", 1).otherwise(0))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

with_dims = (
    islands
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.amount").alias("order_amount"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.shipping_fee"), F.col("o.order_date"), F.col("o.discount"))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_session_tier"),
        "method",
    )
    .agg(
        F.max("paid_at").alias("last_seen"),
        F.countDistinct("method").alias("distinct_methods"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_rows"),
        F.percentile_approx("is_new_session", 0.9).alias("is_new_session_p90"),
    )
    .withColumn("distinct_methods_dense_rank", F.dense_rank().over(w3))
)
