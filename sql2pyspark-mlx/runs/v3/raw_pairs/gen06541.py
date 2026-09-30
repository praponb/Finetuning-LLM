from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 78)
)
