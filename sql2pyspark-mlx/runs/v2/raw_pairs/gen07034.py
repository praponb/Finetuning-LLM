from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg((F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"))
)
