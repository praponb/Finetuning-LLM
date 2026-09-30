from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.sale_date") < "2023-11-01")
    .groupBy(F.col("s.city"), F.col("s.country"))
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
    .filter(F.col("stddev_revenue") >= 53)
    .orderBy("stddev_revenue")
)
