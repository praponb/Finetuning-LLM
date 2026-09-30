from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.avg_quantity") >= 190)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("ps.quantity_range"),
        F.col("ps.avg_quantity"),
    )
    .orderBy(F.col("cost").desc())
)
