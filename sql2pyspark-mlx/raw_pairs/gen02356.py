from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(~(F.col("store_name").like("O%") & (F.col("sqft") >= 10776) & F.col("region").isin(["West", "South", "Central"])))
)

agg = (
    recent_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"))
)

result = (
    agg
    .filter(F.col("west_count") < 35)
    .select("opened_month", "west_count")
)
