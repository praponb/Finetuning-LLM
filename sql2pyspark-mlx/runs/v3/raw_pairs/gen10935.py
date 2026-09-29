from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(8239, 10201))
    .groupBy(F.col("s.city"))
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_stores"))
    .filter(F.col("unique_stores") > 1)
)
