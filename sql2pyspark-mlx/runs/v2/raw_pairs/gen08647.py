from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("country") != "TH") & F.month("signup_date").isin([1, 5, 6]))
    .withColumn("city_upper", F.upper("city"))
)

agg = (
    filtered_customers
    .groupBy("city", "is_active")
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("age")).otherwise(0)).alias("consumer_age"),
        F.max("age").alias("max_age"),
    )
)

result = agg.select("city", "is_active", "consumer_age", "max_age")
