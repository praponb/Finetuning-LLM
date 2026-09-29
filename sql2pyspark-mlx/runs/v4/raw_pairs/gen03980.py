from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("month", "opened_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft"))).alias("bangkok_total_sqft"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "Central", 1)), F.lit(0)), 2).alias("sqft_per_central"),
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
    )
)
