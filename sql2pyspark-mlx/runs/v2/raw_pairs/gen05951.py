from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%own%") & F.col("city").isin(["Bangkok", "Singapore", "Tokyo"]))
    .groupBy("region", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.max("opened_date").alias("last_opened_date"))
)
