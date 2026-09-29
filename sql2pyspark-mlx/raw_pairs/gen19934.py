from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.month("opened_date").isin([3, 5, 9]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.floor("sqft").alias("sqft_whole"),
        F.initcap(F.trim("region")).alias("region_title"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

matched_keys = (
    base
    .select("city", "region")
    .intersect(
        df_stores
        .filter(F.upper("store_name").like("%OWN%"))
        .select("city", "region")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_semi")
)

base_stores = (
    not_flagged
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "sqft_whole",
        "region_title",
        "region_clean",
        (F.datediff(F.current_date(), "opened_date") / 365.25).alias("years_since_opened_date"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean_2"),
    )
)

kept = (
    base_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 2, 3, 4])), "left_anti")
)

result = (
    kept
    .groupBy("city")
    .agg(
        F.percentile_approx("sqft_whole", 0.95).alias("sqft_whole_p95"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
    )
    .orderBy(F.col("avg_sqft").desc())
)
