from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.when(F.col("o.channel").isin(["partner", "mobile"]), "Group A").otherwise("Other").alias("channel_group"),
    )
    .orderBy(F.col("discount").desc(), F.col("channel_group").desc())
)
