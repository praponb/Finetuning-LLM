from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("region") == "North") | F.col("city").isin(["Chiang Mai", "London"]))
)

city_summary = (
    base
    .groupBy("city", F.month("opened_date").alias("opened_month"))
    .agg(F.max("opened_date").alias("latest_opened_date"))
)

result = city_summary.select("city", "opened_month")
