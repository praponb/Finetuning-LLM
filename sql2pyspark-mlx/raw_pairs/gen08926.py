from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-31").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

matched_keys = (
    valid
    .select("region")
    .subtract(
        df_stores
        .filter(F.col("city").isin(["London", "Tokyo"]))
        .select("region")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= "2024-01-11"), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.discount").between(0.15, 0.35)), "left_anti")
)

kept_keys = (
    not_flagged
    .select("city")
    .intersectAll(
        df_stores
        .filter(F.col("region").isin(["Central", "West", "South"]))
        .select("city")
    )
)

kept_rows_stores = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id") == 2), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("sqft") >= 17116, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
    .orderBy(F.col("num_regions").desc_nulls_first())
)
