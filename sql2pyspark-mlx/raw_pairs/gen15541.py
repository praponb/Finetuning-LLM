from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .filter(F.col("avg_revenue") > 2900)
    .orderBy(F.col("avg_revenue").desc(), "opened_month")
    .limit(1)
)
