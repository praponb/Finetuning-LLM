from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy(F.year("opened_date"), "region").orderBy(F.col("sqft").desc())
w_grp = Window.partitionBy("region").orderBy("opened_date", "id")
w_running = Window.partitionBy("region").orderBy("event_seq", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.orderBy("sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select(
        "*",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.percent_rank().over(w2).alias("sqft_percent_rank"),
        F.ntile(10).over(w3).alias("sqft_bucket"),
        F.dense_rank().over(w4).alias("sqft_dense_rank"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

enriched_stores = (
    enriched
    .withColumn("last3_min_sqft_bucket", F.min("sqft_bucket").over(w_running))
    .withColumn("last3_max_sqft_bucket", F.max("sqft_bucket").over(w_running))
    .withColumn("last3_sum_sqft_bucket", F.sum("sqft_bucket").over(w_running))
    .withColumn("last3_avg_sqft_bucket", F.avg("sqft_bucket").over(w_running))
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w_part))
)

filtered = (
    enriched_stores
    .filter((F.col("opened_date") > F.lit("2023-06-11").cast("date")) & F.col("region").isNotNull())
    .select("*", F.concat_ws("-", "city", "region").alias("city_region_key"))
)

base = (
    filtered
    .filter(F.col("opened_date") > F.lit("2023-05-21").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft_dense_rank",
        "event_seq",
        "last3_sum_sqft_bucket",
        (F.col("sqft_percent_rank") + F.col("cumulative_sqft")).alias("sqft_percent_rank_plus_cumulative_sqft"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("sqft_percent_rank_plus_cumulative_sqft") >= 63, "HIGH").otherwise("LOW").alias("sqft_percent_rank_plus_cumulative_sqft_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.round(F.avg("last3_sum_sqft_bucket"), 2).alias("avg_last3_sum_sqft_bucket"),
        F.round(F.sum("sqft_dense_rank") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("sqft_dense_rank_per_singapore"),
    )
    .orderBy(F.col("avg_last3_sum_sqft_bucket").desc_nulls_first())
)
