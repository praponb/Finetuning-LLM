from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.sqft") == 6694)
    .groupBy(
        F.month(F.col("stores.opened_date")).alias("opened_month"),
        F.year(F.col("stores.opened_date")).alias("opened_year"),
    )
    .agg(F.round(F.avg(F.col("store_sales.revenue")), 2).alias("avg_revenue"))
)
