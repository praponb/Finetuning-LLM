from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("o.shipping_fee") > 21)
    .groupBy(
        F.when(F.col("p.category").isin(["Grocery", "Books"]), "tier_1").otherwise("other").alias("category_group"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy("category_group", "row_count")
)
