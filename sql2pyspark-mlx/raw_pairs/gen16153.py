from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

scored = (
    df_stores
    .filter(F.col("sqft") < 10365)
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy("region")
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.countDistinct("region").alias("distinct_regions"),
        F.sum(F.when(F.col("region") == "West", F.col("prev_sqft")).otherwise(0)).alias("west_prev_sqft"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("west_prev_sqft") >= 0)
    .orderBy(F.col("sqft_bit_or").desc_nulls_first())
)
