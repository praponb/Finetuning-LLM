from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("segment") == "Consumer") & F.col("email").isNotNull() & F.col("city").isNotNull())
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

customers_stats = (
    base
    .groupBy("is_active", "country")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
)

result = (
    customers_stats
    .select("is_active", "country", "cnt", "consumer_count")
    .orderBy(F.col("cnt").desc(), "is_active", "country")
    .limit(50)
)
