from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("key")

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("key", "v"))
)

filtered = (
    lines
    .filter(~F.col("key").eqNullSafe(F.lit("device")))
    .select(
        "*",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.initcap(F.trim("key")).alias("key_title"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

base = (
    filtered
    .filter(F.col("key").isin(["utm_source", "weight_kg", "gift"]))
    .select(
        "coupon_codes",
        "key",
        "sorted_coupon_codes",
        "key_clean",
        F.array_join("sorted_coupon_codes", "|").alias("sorted_coupon_codes_text"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean_2"),
    )
)

cleaned = (
    base
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "*",
        F.substring("key", 1, 2).alias("key_prefix"),
        F.size("sorted_coupon_codes").alias("sorted_coupon_codes_count"),
    )
)

with_windows = (
    cleaned
    .select(
        "coupon_codes",
        "sorted_coupon_codes",
        "sorted_coupon_codes_text",
        "key_prefix",
        "sorted_coupon_codes_count",
        F.stddev_samp("sorted_coupon_codes_count").over(w_hist).alias("key_stddev_samp_sorted_coupon_codes_count"),
        F.stddev_samp("sorted_coupon_codes_count").over(w_hist).alias("key_stddev_samp_sorted_coupon_codes_count_2"),
    )
)

result = (
    with_windows
    .select(
        "coupon_codes",
        "sorted_coupon_codes_text",
        "key_prefix",
        "sorted_coupon_codes_count",
        "key_stddev_samp_sorted_coupon_codes_count_2",
    )
    .orderBy(F.col("key_stddev_samp_sorted_coupon_codes_count_2").desc_nulls_last())
)
