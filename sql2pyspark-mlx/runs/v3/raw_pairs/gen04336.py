from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("store_name").like("%Kios%") & F.col("city").isin(["London", "Bangkok"]) & F.col("store_name").like("Ai%"))
    .withColumn("sqft_num", F.col("sqft").cast("string"))
)

summary = (
    filtered_stores
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)

result = (
    summary
    .filter(F.col("sqft_count") > 4)
    .orderBy("stddev_sqft", "opened_month")
)
