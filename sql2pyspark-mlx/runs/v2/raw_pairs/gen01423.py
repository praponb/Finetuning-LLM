from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("o.status").isin(["CANCELLED", "RETURNED", "PENDING"]) & (F.col("p.launch_date") > "2023-11-01"))
    .select(
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("o.customer_id"),
        F.col("p.category"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("category").desc(), F.col("quantity").desc())
)
