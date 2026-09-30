from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("region")
    .intersectAll(
        df_stores
        .filter(F.col("region").isin(["Central", "East", "West"]))
        .select("region")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("opened_date") >= F.lit("2023-09-06").cast("date")) & (F.col("city") == "Chiang Mai"))
    .select(
        "id",
        "store_name",
        "sqft",
        F.substring("city", 1, 4).alias("city_prefix"),
        F.lower("city").alias("city_lower"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

base_stores = (
    base
    .filter(F.col("sqft").between(12722, 14524))
    .select("*", F.initcap(F.trim("store_name")).alias("store_name_title"))
)

base_stores_2 = (
    base_stores
    .filter(F.col("sqft").between(6205, 9956))
    .select("*", F.substring("store_name", 1, 2).alias("store_name_prefix"))
)

eligible = (
    base_stores_2
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.units") >= 38), "left_anti")
)

result = (
    eligible
    .select("id", "region_filled", "store_name_title")
    .orderBy(F.col("id").asc_nulls_last())
)
