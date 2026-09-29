from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("i.store_id").isin([1, 2, 4]))
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
    .orderBy("unique_products")
)
