from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc(), "category")
    .limit(10)
)
