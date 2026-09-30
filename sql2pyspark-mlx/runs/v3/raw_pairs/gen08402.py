from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.supplier_name").isNull())
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("order_id").desc(), "id")
)
