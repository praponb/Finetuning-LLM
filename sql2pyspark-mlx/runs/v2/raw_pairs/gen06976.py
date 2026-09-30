from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.stock_qty").between(120, 134))
    .groupBy(F.col("s.city"))
    .agg(F.min(F.col("ss.revenue")).alias("min_revenue"))
)
