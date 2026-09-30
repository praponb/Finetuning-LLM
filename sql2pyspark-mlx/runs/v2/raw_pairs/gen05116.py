from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("id").desc(), "product_name")
    .limit(3)
)
