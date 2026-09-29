from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(
        F.when(F.col("s.region").isin(["Central", "East"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.count("*").alias("inventory_count"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc(), "region_group")
    .limit(20)
)
