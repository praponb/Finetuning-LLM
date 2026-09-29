from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_anti")
    .filter(F.col("p.cost").between(390, 610))
    .select(F.col("p.id"), F.col("p.product_name"), F.col("p.supplier_id"))
)
