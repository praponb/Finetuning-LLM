from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
    .filter(F.col("revenue_count") >= 3)
)
