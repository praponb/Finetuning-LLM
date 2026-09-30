from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount").between(1240, 1290) & F.col("oi.product_id").isin([1, 7]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.status"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(20)
)
