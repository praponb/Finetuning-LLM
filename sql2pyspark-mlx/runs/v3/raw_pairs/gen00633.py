from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") != "Bangkok")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .filter(F.col("max_units") >= 24)
)
