from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("country").isin(["SG", "UK", "TH"]) & F.col("email").like("%@yahoo.com")) | F.col("city").isNotNull())
)

city_summary = (
    recent_customers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0)).alias("consumer_count"),
    )
)

result = (
    city_summary
    .filter(F.col("consumer_count") > 71)
    .select("city", "consumer_count")
)
