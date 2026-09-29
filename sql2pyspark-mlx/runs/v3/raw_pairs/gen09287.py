from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.col("p.cost") < 320) | (F.col("p.category") == "Fashion"))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.array_contains(F.col("p.tags"), "sale").alias("is_sale"),
    )
)
