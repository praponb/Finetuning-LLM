from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region").orderBy("sqft", "id")
w_ordered = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last())

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("city_region_key", F.concat_ws("/", "city", "region"))
    .withColumn("region_filled", F.coalesce("region", F.lit("OTHER")))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_name", "region", "opened_date", "sqft", "region_filled")
)

valid_stores = (
    deduped
    .filter(F.col("region").isNotNull() & F.col("region").isin(["East", "Central"]))
    .select(
        "*",
        F.regexp_replace("store_name", " ", "_").alias("store_name_slug"),
        F.length("region").alias("region_length"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

metrics = (
    valid_stores
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "store_name_slug",
        "region_length",
        "region_clean",
        F.min("region_length").over(w2).alias("region_min_region_length"),
        F.ntile(10).over(w3).alias("sqft_bucket"),
        F.avg("region_length").over(w_ordered).alias("region_length_avg_6_prior"),
        F.cume_dist().over(w5).alias("sqft_cume_dist"),
    )
)

result = (
    metrics
    .withColumn("region_min_region_length_tier", F.when(F.col("region_min_region_length") >= 89, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["region_min_region_length_tier", "region"], ["region_min_region_length_tier"]],
        "region_min_region_length_tier",
        "region",
    )
    .agg(
        F.percentile_approx("sqft_cume_dist", 0.9).alias("sqft_cume_dist_p90"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
        F.grouping("region_min_region_length_tier").alias("grouping_level"),
    )
    .orderBy(F.col("region_min_region_length_tier").desc_nulls_first())
)
