from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.year(F.col("i.last_restock")) == 2023)
    .select(
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("p.price"),
        F.col("s.rating"),
        F.date_format(F.col("i.last_restock"), "yyyy-MM-dd").alias("last_restock_label"),
    )
    .orderBy("last_restock_label")
)
