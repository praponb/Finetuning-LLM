from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.month("opened_date").isin([6, 10, 11]) & (F.col("sqft") > 13992) & F.col("store_name").like("Sia%"))
)

agg = (
    valid_stores
    .groupBy("city", "region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
)

result = (
    agg
    .filter(F.col("avg_sqft") >= 51)
    .select("city", "region", "avg_sqft", "min_opened_date")
)
