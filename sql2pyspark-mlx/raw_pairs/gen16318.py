from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w_recent = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("region", "city").orderBy("sqft_str", "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.col("sqft").cast("double").alias("sqft_str"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id") == 2), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    ranked
    .filter(F.col("store_name").like("%Old%"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_str",
        F.least("sqft", "sqft_str").alias("least_sqft_sqft_str"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "region",
        F.max("sqft").over(w_recent).alias("last3_max_sqft"),
        F.min("sqft").over(w_recent).alias("last3_min_sqft"),
        F.count("sqft").over(w_recent).alias("last3_count_sqft"),
        F.ntile(10).over(w_running).alias("sqft_str_bucket"),
    )
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("last3_count_sqft") >= 13, "PREMIUM").otherwise("STANDARD").alias("last3_count_sqft_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum("last3_count_sqft").alias("total_last3_count_sqft"),
    )
)

result = (
    daily
    .select("last3_count_sqft_tier")
    .orderBy(F.col("last3_count_sqft_tier").desc())
    .limit(10)
)
