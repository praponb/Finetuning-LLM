from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.customer_id") == 18) & F.col("o.customer_id").isNotNull())
    .select(
        F.col("oi.unit_price"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("o.customer_id"),
        F.col("o.amount"),
    )
    .orderBy(F.col("amount").desc(), F.col("customer_id").desc())
)
