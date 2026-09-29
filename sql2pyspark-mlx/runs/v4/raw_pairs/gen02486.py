from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w4 = Window.partitionBy("region", "city").orderBy("cumulative_sqft", "id")
w5 = Window.orderBy("rank_in_group", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w7 = Window.partitionBy("city", "region").orderBy(F.col("sqft_bucket").asc_nulls_first(), "id")
w8 = Window.partitionBy("city").orderBy("cumulative_sqft", "id")
w9 = Window.partitionBy("city")

enriched = (
    df_stores
    .withColumn("cumulative_sqft", F.sum("sqft").over(w))
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w2))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .withColumn("prev_cumulative_sqft", F.lag("cumulative_sqft", 1).over(w4))
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w5))
    .withColumn("sqft_bucket", F.ntile(4).over(w6))
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

enriched_stores = (
    latest
    .withColumn("prev_sqft_bucket", F.lag("sqft_bucket", 1).over(w8))
    .withColumn("sqft_avg_6_prior_share", F.round((F.col("sqft_avg_6_prior") / F.sum("sqft_avg_6_prior").over(w9)), 4))
)

cleaned = (
    enriched_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-19").cast("date")) & F.col("cumulative_sqft").between(60445, 65416))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "rank_in_group",
        "sqft_avg_6_prior",
        "sqft_bucket",
        "latest_rank",
        "prev_sqft_bucket",
        "sqft_avg_6_prior_share",
        (F.col("cumulative_sqft") * F.col("sqft_avg_6_prior_share")).alias("cumulative_sqft_times_sqft_avg_6_prior_share"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

result = (
    cleaned
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("sqft_bucket"))).alias("west_total_sqft_bucket"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft_avg_6_prior_share")).otherwise(0)).alias("north_sqft_avg_6_prior_share"),
    )
    .orderBy(F.col("west_total_sqft_bucket").asc_nulls_last())
)
