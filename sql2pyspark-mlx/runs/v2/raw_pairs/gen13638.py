from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("last_restock") < "2025-05-01")
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_quantity") > 31)
    .select(
        F.col("products.cost"),
        F.col("t.latest_last_restock"),
        F.col("t.avg_quantity"),
    )
    .orderBy(F.col("cost").desc(), "latest_last_restock")
)
