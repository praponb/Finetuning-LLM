from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

matched_keys = (
    base
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("opened_date") < "2023-09-01")
        .select("id", "city")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 3, 5, 6])), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < "2024-01-01"), "left_semi")
)

filtered = (
    kept
    .filter(F.col("city") != "Chiang Mai")
    .select("*", F.coalesce("sqft", F.lit(0)).alias("sqft_filled"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("sqft_filled") >= 76, "PREMIUM").otherwise("STANDARD").alias("sqft_filled_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft_filled"))).alias("chiang_mai_total_sqft_filled"),
        F.max("sqft").alias("max_sqft"),
        F.countDistinct("region").alias("distinct_regions"),
    )
    .filter(F.col("max_sqft") > 0)
    .orderBy(F.col("chiang_mai_total_sqft_filled").desc())
)
