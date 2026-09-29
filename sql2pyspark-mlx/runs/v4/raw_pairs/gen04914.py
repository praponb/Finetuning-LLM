from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.cost"),
    )
    .orderBy(F.col("id").desc(), F.col("cost").desc())
)
