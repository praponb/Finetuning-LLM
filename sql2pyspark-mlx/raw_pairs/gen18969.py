from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("p.stock_qty") < 385)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("p.launch_date"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)
