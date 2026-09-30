from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .filter(F.col("sqft_range") < 20)
)
