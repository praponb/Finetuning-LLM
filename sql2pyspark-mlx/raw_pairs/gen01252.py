from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

flattened = lines.select("id", "attr_key", F.explode_outer("coupon_codes").alias("tag"))

base = (
    flattened
    .filter(F.col("tag").isNotNull())
    .select(
        "*",
        F.lower("attr_key").alias("attr_key_lower"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_clean"),
    )
)

valid = (
    base
    .filter(F.col("tag").isNotNull())
    .select(
        "attr_key",
        "tag",
        "attr_key_lower",
        "attr_key_clean",
        F.upper("tag").alias("tag_upper"),
        F.length("attr_key").alias("attr_key_length"),
    )
)

with_windows = (
    valid
    .select("*", F.count("attr_key_length").over(w).alias("tag_count_attr_key_length"))
)

result = (
    with_windows
    .groupBy("tag")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("attr_key_length") / F.nullif(F.count(F.when(F.col("tag") == "BUNDLE", 1)), F.lit(0)), 2).alias("attr_key_length_per_bundle"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_count"),
        F.max("attr_key_length").alias("max_attr_key_length"),
        F.round(F.sum("tag_count_attr_key_length") / F.nullif(F.count(F.when(F.col("tag") == "SPRING10", 1)), F.lit(0)), 2).alias("tag_count_attr_key_length_per_spring10"),
    )
    .filter(F.col("max_attr_key_length") > 0)
    .orderBy(F.col("max_attr_key_length").desc())
)
