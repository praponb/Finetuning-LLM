from pyspark.sql import functions as F

flattened = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("key", "v"))

lines = flattened.select("id", "key", F.explode_outer("coupon_codes").alias("element"))

kept_keys = (
    lines
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("created_at") >= "2025-06-01")
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("key") == "utm_source")
    .withColumn("key_length", F.length("key"))
)

result = (
    valid
    .groupBy("key")
    .agg(
        F.round(F.sum("key_length") / F.nullif(F.count(F.when(F.col("key") == "device", 1)), F.lit(0)), 2).alias("key_length_per_device"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.round(F.sum("key_length") / F.nullif(F.count(F.when(F.col("element") == "BUNDLE", 1)), F.lit(0)), 2).alias("key_length_per_bundle"),
    )
    .filter(F.col("key_length_per_bundle") > 0)
    .orderBy(F.col("key").desc())
)
