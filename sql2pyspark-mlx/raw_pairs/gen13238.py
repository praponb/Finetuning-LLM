from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.count("*").alias("total_count"),
    )
)
