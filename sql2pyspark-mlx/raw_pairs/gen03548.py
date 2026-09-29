from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
        F.when(F.col("o.channel").isin(["store", "web"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(F.count("*").alias("total_count"))
    .orderBy("channel_group", "order_month")
    .limit(5)
)
