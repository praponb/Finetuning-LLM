from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("i.last_restock")).isin([1, 7, 9]))
    .groupBy(F.col("p.category"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("total_quantity").desc(), "category")
    .limit(10)
)
