from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft_whole").desc(), "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("running_sqft_whole").desc_nulls_first())
w4 = Window.partitionBy("region").orderBy(F.col("sqft_whole").desc_nulls_first())
w5 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-07-31").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.floor("sqft").alias("sqft_whole"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "region",
        "sqft_whole",
        "resolved_region",
        F.ntile(3).over(w).alias("sqft_whole_bucket"),
        F.sum("sqft_whole").over(w2).alias("running_sqft_whole"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    ranked
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "sqft_whole",
        "resolved_region",
        "running_sqft_whole",
        "dedup_rank",
        F.coalesce("region", F.lit("unassigned")).alias("region_filled"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region_2"),
    )
)

enriched_stores = (
    base
    .filter(F.col("dedup_rank") <= 1)
    .withColumn("sqft_whole_rank", F.rank().over(w4))
    .withColumn("running_sqft_whole_cume_dist", F.cume_dist().over(w3))
    .withColumn("first_known_running_sqft_whole", F.first("running_sqft_whole", ignorenulls=True).over(w5))
)

daily = (
    enriched_stores
    .cube("region")
    .agg(
        F.round(F.sum("running_sqft_whole_cume_dist") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("running_sqft_whole_cume_dist_per_west"),
        F.avg(F.when(F.col("region") == "Central", F.col("running_sqft_whole_cume_dist"))).alias("central_avg_running_sqft_whole_cume_dist"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("sqft_whole_rank", 0.5).alias("sqft_whole_rank_p50"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    daily
    .select(
        "running_sqft_whole_cume_dist_per_west",
        "central_avg_running_sqft_whole_cume_dist",
        "sqft_whole_rank_p50",
        "gid",
    )
    .orderBy("running_sqft_whole_cume_dist_per_west")
)
