from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.last_restock") > "2025-01-01")
    .groupBy(F.col("p.category"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("avg_quantity").desc(), "quantity_range")
)
