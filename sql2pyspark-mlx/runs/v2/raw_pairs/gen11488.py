from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.pct_kerry") > 78)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("ot.pct_kerry"),
    )
    .orderBy(F.col("id").desc(), "discount")
    .limit(3)
)
