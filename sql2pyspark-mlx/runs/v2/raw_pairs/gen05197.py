from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .filter(F.col("oi.product_id").isNotNull())
    .join(df_products.filter(F.col("category").isin(["Fashion", "Electronics"])).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_anti")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
    .orderBy(F.col("product_id").desc(), "quantity")
)
