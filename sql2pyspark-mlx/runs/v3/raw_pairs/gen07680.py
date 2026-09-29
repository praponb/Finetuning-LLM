from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .filter(F.col("min_quantity") > 130)
    .orderBy("min_quantity")
)
