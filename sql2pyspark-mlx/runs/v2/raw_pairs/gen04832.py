from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.quantity").between(107, 214))
    .groupBy(F.col("s.region"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy("quantity_count", "region")
    .limit(10)
)
