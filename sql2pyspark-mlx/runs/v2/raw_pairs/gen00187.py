from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("city").isin(["London", "New York", "Tokyo"]) & (F.col("region") != "South") & F.col("city").isin(["New York", "Singapore"]))
    .withColumn("opened_date_plus_68d", F.date_add("opened_date", 68))
)

agg = (
    filtered_stores
    .groupBy("city")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
)

result = (
    agg
    .filter(F.col("avg_sqft") >= 32)
    .select("city", "avg_sqft")
    .orderBy(F.col("avg_sqft").desc(), "city")
    .limit(3)
)
