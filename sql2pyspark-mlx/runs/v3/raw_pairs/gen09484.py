from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 3)
)
