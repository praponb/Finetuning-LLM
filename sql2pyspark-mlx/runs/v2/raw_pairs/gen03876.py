from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand").isin(["Stark", "Umbrella", "Acme"]))
    .groupBy(F.col("s.city"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy("city", "quantity_range")
)
