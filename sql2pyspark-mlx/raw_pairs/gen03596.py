from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating") > 2.8)
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
)
