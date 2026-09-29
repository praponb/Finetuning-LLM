from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([5, 6, 7, 10]))
    .groupBy(F.col("s.country"), F.col("s.city"))
    .agg(
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("city").desc())
)
