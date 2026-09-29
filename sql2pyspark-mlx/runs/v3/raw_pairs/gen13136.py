from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.launch_date") <= "2023-05-24") & F.array_contains(F.col("p.tags"), "premium"))
    .select(
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("p.cost"),
        F.col("s.city"),
    )
)
