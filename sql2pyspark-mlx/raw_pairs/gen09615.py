from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy(F.col("sqft_share").desc_nulls_last())
w5 = Window.partitionBy("city").orderBy("sqft_share", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("opened_date").asc_nulls_last(), "id")

cleaned = (
    df_stores
    .filter((F.length("store_name") < 12) & (F.col("opened_date") > F.to_date(F.lit("2023-07-10"))))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.substring("store_name", 1, 3).alias("store_name_prefix"),
        F.initcap(F.trim("region")).alias("region_title"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

with_windows = (
    cleaned
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("prev_sqft", F.lag("sqft", 2, 0).over(w2))
)

scored = (
    with_windows
    .withColumn("last_known_sqft_share", F.last("sqft_share", ignorenulls=True).over(w3))
    .withColumn("sqft_share_cume_dist", F.cume_dist().over(w_running))
)

scored_stores = (
    scored
    .filter(F.col("sqft") == 6416)
    .select(
        "*",
        F.min("sqft_share_cume_dist").over(w5).alias("last4_min_sqft_share_cume_dist"),
        F.avg("sqft_share_cume_dist").over(w5).alias("last4_avg_sqft_share_cume_dist"),
        F.sum("sqft_share_cume_dist").over(w5).alias("last4_sum_sqft_share_cume_dist"),
        F.max("sqft_share_cume_dist").over(w5).alias("last4_max_sqft_share_cume_dist"),
    )
)

latest = (
    scored_stores
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

grouped = (
    latest
    .groupBy("city")
    .agg(
        F.round(F.sum("sqft_share_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("sqft_share_cume_dist_per_new_york"),
        F.avg(F.when(F.col("city") == "New York", F.col("sqft"))).alias("new_york_avg_sqft"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(F.avg("sqft_share_cume_dist"), 2).alias("avg_sqft_share_cume_dist"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count_2"),
        F.percentile_approx("last_known_sqft_share", [0.25, 0.5, 0.75]).alias("last_known_sqft_share_quartiles"),
    )
)

result = (
    grouped
    .select(
        "city",
        "sqft_share_cume_dist_per_new_york",
        "london_count_2",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
)
