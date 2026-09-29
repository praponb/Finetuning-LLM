from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .groupingSets([["week_start", "region"], ["week_start"]], "week_start", "region")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft"))).alias("singapore_total_sqft"),
        F.min("sqft").alias("min_sqft"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.grouping("week_start").alias("grouping_id"),
    )
)
