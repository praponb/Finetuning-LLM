from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(~F.col("s.supplier_name").like("P%"))
    .groupBy(F.col("p.supplier_id"))
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .filter(F.col("avg_revenue") > 2000)
    .orderBy(F.col("avg_revenue").desc(), "supplier_id")
    .limit(5)
)
