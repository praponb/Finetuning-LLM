from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.month(F.col("p.launch_date")).alias("launch_month"),
        F.col("p.supplier_id"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .filter(F.col("min_quantity") > 170)
)
