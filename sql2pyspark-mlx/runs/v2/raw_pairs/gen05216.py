from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"))
    .filter(F.col("london_count") >= 48)
)
