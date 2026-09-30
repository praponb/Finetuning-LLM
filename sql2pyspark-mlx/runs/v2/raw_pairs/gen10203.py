from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.product_name").like("US%"))
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
    .filter(F.col("avg_units") > 40)
    .orderBy("city", "region")
)
