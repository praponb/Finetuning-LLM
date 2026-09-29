from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_amount", "id")
w2 = Window.partitionBy("channel").orderBy("order_amount", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")
w4 = Window.orderBy("prev_order_amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("prev_shipping_fee", "id")

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-11-13").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.shipping_fee"),
        F.col("p.status").alias("order_status"),
        F.col("p.order_date"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.status_group",
        "src.shipping_fee",
        F.col("o.channel"),
        F.col("o.amount").alias("order_amount"),
    )
)

enriched_payments = (
    joined
    .select(
        "id",
        "status_group",
        "shipping_fee",
        "channel",
        F.lag("order_amount").over(w).alias("prev_order_amount"),
        F.lead("order_amount").over(w).alias("next_order_amount"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.row_number().over(w).alias("seq_no"),
        F.avg("order_amount").over(w2).alias("order_amount_avg_5"),
    )
)

latest = (
    enriched_payments
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "status_group",
        "shipping_fee",
        "prev_order_amount",
        "prev_shipping_fee",
    )
)

valid = (
    latest
    .filter(F.col("prev_shipping_fee").isNotNull())
    .select("*", F.ceil("prev_order_amount").alias("prev_order_amount_whole"))
)

enriched_payments_2 = (
    valid
    .withColumn("last_known_prev_order_amount_whole", F.last("prev_order_amount_whole", ignorenulls=True).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

result = (
    enriched_payments_2
    .select(
        "id",
        "status_group",
        "shipping_fee",
        "prev_order_amount",
        "prev_shipping_fee",
        "prev_order_amount_whole",
        "last_known_prev_order_amount_whole",
        "row_seq",
    )
    .orderBy(F.col("prev_shipping_fee").desc())
)
