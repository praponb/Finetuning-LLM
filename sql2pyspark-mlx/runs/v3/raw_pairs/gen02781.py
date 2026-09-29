from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.product_id").isin([3, 7]))
    .groupBy(F.col("p.brand"), F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(F.countDistinct(F.col("ss.units")).alias("unique_products"))
    .filter(F.col("unique_products") > 1)
)
