from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.store_name").like("Ol%"))
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.when(F.col("s.sqft") >= 12355, "high").when(F.col("s.sqft") >= 2940, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .orderBy("sqft_bucket")
)
