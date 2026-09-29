from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "outer")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(~F.col("s.store_name").like("Air%"))
    .select(
        F.col("i.last_restock"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("p.category"),
        F.col("p.supplier_id"),
    )
    .orderBy("supplier_id")
)
