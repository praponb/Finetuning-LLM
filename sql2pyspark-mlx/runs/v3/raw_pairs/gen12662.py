from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.quantity") >= 212)
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy("supplier_id", F.col("total_quantity").desc())
)
