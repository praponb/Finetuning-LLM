from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("s.city").isNotNull())
    .groupBy(F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
)
