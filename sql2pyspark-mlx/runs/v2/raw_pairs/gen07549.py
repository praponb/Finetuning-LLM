from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .filter(F.col("oi.product_id").isNotNull())
    .join(df_products.filter(F.col("supplier_id").isin([14, 29, 35])).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("oi.unit_price").between(150, 280))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
    )
)
