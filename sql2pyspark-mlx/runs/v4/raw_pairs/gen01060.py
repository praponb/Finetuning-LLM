from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.quantity_range") > 69)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("ps.quantity_range"),
    )
    .orderBy(F.col("cost").desc(), "id")
    .limit(10)
)
