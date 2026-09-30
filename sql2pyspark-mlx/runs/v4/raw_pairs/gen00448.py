from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w_running = Window.partitionBy("status").orderBy(F.col("discount").asc_nulls_last())
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_part = Window.partitionBy("status").orderBy("order_amount", "id").rowsBetween(-2, -1)
w_recent = Window.orderBy("amount", "id")
w_recent_2 = Window.partitionBy("order_id").orderBy(F.col("method_stddev_samp_shipping_fee").desc(), "id")
w7 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w8 = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("method").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method", "order_id").orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2024-02-15 19:00:00"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.concat("method", F.lit(" - "), "status").alias("method_status"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.discount"))
)

enriched_payments = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.shipping_fee"), F.col("p.amount").alias("order_amount"))
)

scored = (
    enriched_payments
    .select(
        "*",
        F.stddev_samp("shipping_fee").over(w).alias("method_stddev_samp_shipping_fee"),
        F.dense_rank().over(w_running).alias("discount_dense_rank"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_90d"),
        F.avg("shipping_fee").over(w_part).alias("shipping_fee_avg_2_prior"),
    )
)

with_windows = (
    scored
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent_2))
    .withColumn("seq_no_2", F.row_number().over(w7))
)

scored_payments = (
    with_windows
    .withColumn("order_amount_avg_6", F.avg("order_amount").over(w8))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w9))
    .withColumn("shipping_fee_avg_2_prior_avg_6", F.avg("shipping_fee_avg_2_prior").over(w10))
)

result = (
    scored_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "method_status",
        "method_clean",
        "shipping_fee",
        "method_stddev_samp_shipping_fee",
        "discount_dense_rank",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "GROUP_A").when(F.col("status").rlike("^(FAILED)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
        F.col("max_payment_amount"),
    )
)
