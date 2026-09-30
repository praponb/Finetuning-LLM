from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-06").cast("date")) & (F.col("city") != "Berlin"))
    .select(
        "*",
        F.lower("city").alias("city_lower"),
        (F.datediff(F.current_date(), "opened_date") / 365).alias("years_since_opened_date"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.units").isNotNull()), "left_anti")
)

kept_keys = (
    not_flagged
    .select("city")
    .exceptAll(
        df_stores
        .filter(F.col("city") != "Tokyo")
        .select("city")
    )
)

reconciled = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("region").eqNullSafe(F.lit("East")))
    .withColumn("years_since_opened_date_per_sqft", F.round((F.col("years_since_opened_date") / F.col("sqft")), 2))
)

result = (
    valid
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .groupingSets([["city", "quarter_start"], ["city"]], "city", "quarter_start")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.avg("years_since_opened_date_per_sqft"), 2).alias("avg_years_since_opened_date_per_sqft"),
        F.avg("years_since_opened_date").alias("avg_years_since_opened_date"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
