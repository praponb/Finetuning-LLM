from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 5)
)
