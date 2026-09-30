from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)
