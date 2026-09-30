from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.country") != "SG")
    .select(
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("s.city"),
        F.coalesce(F.col("s.rating"), F.lit(0)).alias("rating_filled"),
    )
)
