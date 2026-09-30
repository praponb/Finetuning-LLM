from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .filter(F.col("p.supplier_id").isNotNull() | F.col("p.brand").isin(["Acme", "Umbrella"]))
    .select(
        F.col("oi.product_id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
    .orderBy("product_name")
)
