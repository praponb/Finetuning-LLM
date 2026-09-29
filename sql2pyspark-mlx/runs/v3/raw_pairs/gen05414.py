from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNull() & F.col("p.product_name").isNotNull())
    .select(
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("p.supplier_id"),
    )
    .orderBy("amount")
)
