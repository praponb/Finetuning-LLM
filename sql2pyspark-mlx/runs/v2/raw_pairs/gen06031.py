from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .groupBy(F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_products"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
    .orderBy("avg_units")
)
