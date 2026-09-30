from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.avg_quantity") >= 61)
    .select(F.col("p.cost"), F.col("P.last_last_restock"), F.col("P.avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
