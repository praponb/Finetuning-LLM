from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
)
