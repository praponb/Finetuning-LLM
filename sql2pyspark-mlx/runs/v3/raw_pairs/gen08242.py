from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("p.price") >= 1230, "high").when(F.col("p.price") >= 600, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .filter(F.col("avg_quantity") >= 90)
    .orderBy(F.col("quantity_count").desc(), "price_bucket")
    .limit(20)
)
