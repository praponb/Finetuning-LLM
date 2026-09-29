from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.to_date("paid_at"), "order_status").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("order_shipping_fee").desc_nulls_last())
w4 = Window.partitionBy("method").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_payments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.shipping_fee") > 21)
    .select("a.*", F.col("p.discount"), F.col("p.shipping_fee"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.channel"),
        F.col("o.status").alias("order_status"),
        F.col("o.shipping_fee").alias("order_shipping_fee"),
    )
)

with_windows = (
    with_dims
    .withColumn("running_shipping_fee", F.sum("shipping_fee").over(w2))
    .withColumn("order_shipping_fee_percent_rank", F.percent_rank().over(w3))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("discount") >= 0.4, "HIGH").otherwise("LOW").alias("discount_tier"),
        F.when(F.col("running_shipping_fee") >= 179, "LARGE").otherwise("SMALL").alias("running_shipping_fee_tier"),
    )
    .agg(
        F.sum("order_shipping_fee").alias("total_order_shipping_fee"),
        F.round(F.sum("order_shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("order_shipping_fee_per_refunded"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
)
