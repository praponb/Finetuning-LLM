from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("city") == "Tokyo") & ((F.col("opened_date") <= "2024-04-01") | (F.length("store_name") < 12)))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

agg = (
    src
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)

result = (
    agg
    .select("opened_month", "latest_opened_date")
    .orderBy("latest_opened_date", "opened_month")
    .limit(50)
)
