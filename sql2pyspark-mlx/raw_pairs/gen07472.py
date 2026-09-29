from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date").between("2023-03-16", "2025-08-01"))
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc())
)
