from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") != 9101)
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .filter(F.col("total_quantity") > 1350)
    .orderBy("region", "city")
    .limit(3)
)
