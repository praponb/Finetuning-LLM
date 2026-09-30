from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 14936, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["sqft_tier", "city"], ["sqft_tier"], []], "sqft_tier", "city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.avg(F.when(F.col("city") == "New York", F.col("sqft"))).alias("new_york_avg_sqft"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("sqft_bit_or") > 1)
)
