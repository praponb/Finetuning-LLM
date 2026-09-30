from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(F.sum(F.col("ss.units")).alias("total_units"))
    .filter(F.col("total_units") > 240)
    .orderBy("total_units")
)
