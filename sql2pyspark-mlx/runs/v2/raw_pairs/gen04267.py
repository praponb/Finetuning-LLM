from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.when(F.col("s.sqft") >= 14127, "high").when(F.col("s.sqft") >= 7105, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count("*").alias("store_sale_count"),
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
    )
    .filter(F.col("store_sale_count") >= 4)
    .orderBy("sqft_bucket")
)
