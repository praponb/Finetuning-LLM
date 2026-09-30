from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.launch_date") < "2024-10-01")
    .groupBy(F.col("p.supplier_id"), F.col("p.category"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 5)
)
