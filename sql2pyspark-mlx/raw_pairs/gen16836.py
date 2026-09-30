from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc())

cleaned = (
    df_stores
    .filter(F.col("sqft") > 5836)
    .withColumn("region_city_key", F.concat_ws("-", "region", "city"))
)

kept_keys = (
    cleaned
    .select("region")
    .subtract(
        df_stores
        .filter(F.col("opened_date") >= "2025-01-01")
        .select("region")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

cleaned_stores = (
    verified
    .filter(F.col("region").isNotNull())
    .select(
        "store_name",
        "opened_date",
        "sqft",
        "region_city_key",
        F.trim("store_name").alias("clean_store_name"),
    )
)

with_windows = (
    cleaned_stores
    .filter(F.year("opened_date") >= 2024)
    .select(
        "store_name",
        "opened_date",
        "sqft",
        "clean_store_name",
        F.cume_dist().over(w).alias("sqft_cume_dist"),
    )
)

result = (
    with_windows
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("sqft_cume_dist_tier", F.when(F.col("sqft_cume_dist") >= 0.68, "PREMIUM").otherwise("STANDARD"))
    .cube("year_start", "sqft_cume_dist_tier")
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.count("*").alias("row_count"),
        F.bit_and("sqft").alias("sqft_bit_and_2"),
        F.grouping("year_start").alias("gid"),
    )
)
