from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.lower(F.col("p.product_name")).like("%ois%") & F.col("oi.order_id").isin([2, 14, 15, 20]))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.cost"),
    )
    .orderBy(F.col("cost").desc(), "product_id")
)
