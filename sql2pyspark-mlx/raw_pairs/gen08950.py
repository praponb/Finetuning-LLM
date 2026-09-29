from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("store_name_length").desc_nulls_first())
w_hist = Window.orderBy("store_name_length")
w3 = Window.partitionBy(F.month("opened_date"), "region").orderBy(F.col("store_name_length").asc_nulls_first())
w_running = Window.partitionBy("region")

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.floor("sqft").alias("sqft_whole"),
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

filtered = (
    base
    .filter((F.col("opened_date") > F.lit("2023-09-14").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.when(F.col("sqft_whole") >= 89, "High").when(F.col("sqft_whole") >= 13, "Medium").otherwise("Low").alias("sqft_whole_tier"),
        F.concat("store_name", F.lit(" "), "region").alias("store_name_region"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

matched_keys = (
    filtered
    .select("region", "id")
    .intersectAll(
        df_stores
        .filter(F.length("store_name") >= 5)
        .select("region", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.length("store_name").alias("store_name_length"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled_2"),
    )
)

enriched = (
    cleaned
    .withColumn("store_name_length_cume_dist", F.cume_dist().over(w))
    .withColumn("store_name_length_rank", F.rank().over(w_hist))
    .withColumn("store_name_length_dense_rank", F.dense_rank().over(w3))
    .withColumn("region_count_sqft_whole", F.count("sqft_whole").over(w_running))
    .withColumn("region_max_sqft_whole", F.max("sqft_whole").over(w_running))
)

cleaned_stores = (
    enriched
    .filter((F.col("opened_date") >= F.lit("2023-08-09").cast("date")) & (F.col("store_name_length") <= 38))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft_whole",
        "store_name_region_key",
        "region_clean",
        "sqft_whole_tier",
        "region_filled",
        "region_filled_2",
        "store_name_length_cume_dist",
        "store_name_length_rank",
        "store_name_length_dense_rank",
        "region_max_sqft_whole",
        F.least("store_name_length_dense_rank", "region_max_sqft_whole").alias("least_store_name_length_dense_rank_region_max_sqft_whole"),
    )
)

result = (
    cleaned_stores
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.percentile_approx("least_store_name_length_dense_rank_region_max_sqft_whole", 0.9).alias("least_store_name_length_dense_rank_region_max_sqft_whole_p90"),
        F.percentile_approx("sqft_whole", [0.25, 0.5, 0.75]).alias("sqft_whole_quartiles"),
    )
    .orderBy(
        F.col("least_store_name_length_dense_rank_region_max_sqft_whole_p90").desc_nulls_last(),
    )
)
