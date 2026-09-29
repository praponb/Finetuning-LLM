from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .filter(F.col("store_id").isin([2, 5]))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.cnt") > 7)
    .select(F.col("p.id"), F.col("p.price"), F.col("P.cnt"), F.col("P.min_quantity"))
)
