from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
    )
    .orderBy("max_units")
)
