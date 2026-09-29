from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("row_count"),
    )
)
