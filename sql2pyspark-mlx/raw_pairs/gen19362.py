from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-20").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.round("sqft", 2).alias("sqft_rounded"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.revenue").between(1400, 3000)), "left_semi")
)

matched_keys = (
    kept
    .select("id")
    .intersect(
        df_stores
        .filter(F.length("store_name") < 13)
        .select("id")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id") == 10), "left_semi")
)

matched_keys_2 = (
    eligible
    .select("region", "id")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["Central", "South", "West"]))
        .select("region", "id")
    )
)

verified = (
    eligible
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (~F.col("ch.status").isin(["SHIPPED", "CANCELLED", "PENDING"])), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("sqft") >= 14937, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.round(F.avg("sqft_rounded"), 2).alias("avg_sqft_rounded"),
        F.count_if(F.col("region") == "West").alias("west_events"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("avg_sqft_rounded").desc())
)
