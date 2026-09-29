from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("p.price") >= 960, "high").when(F.col("p.price") >= 470, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.count("*").alias("num_order_items"),
    )
    .orderBy("price_bucket")
)
