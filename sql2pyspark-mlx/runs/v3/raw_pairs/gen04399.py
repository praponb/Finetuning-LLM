from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.launch_date").between("2024-08-01", "2024-09-17"))
    .groupBy(F.col("s.region"))
    .agg(F.sum(F.col("ss.units")).alias("total_units"))
    .orderBy("region")
    .limit(50)
)
