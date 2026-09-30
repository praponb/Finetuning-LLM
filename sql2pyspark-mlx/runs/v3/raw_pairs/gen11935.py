from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.col("price").between(660, 1190)).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.product_id").isNull())
    .select(F.col("oi.id"), F.col("oi.quantity"), F.col("oi.product_id"))
    .orderBy("id")
)
