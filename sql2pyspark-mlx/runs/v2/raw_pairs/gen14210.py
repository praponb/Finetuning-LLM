from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.supplier_id"), F.col("p.brand"))
    .agg(F.avg(F.col("ss.units")).alias("avg_units"))
    .filter(F.col("avg_units") > 19)
)
