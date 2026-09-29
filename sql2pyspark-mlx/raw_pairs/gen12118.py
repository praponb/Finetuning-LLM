from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("attr_key", "value"))
)

flattened = (
    exploded
    .select("id", "attr_key", "value", F.explode("coupon_codes").alias("value_2"))
)

remaining_keys = (
    flattened
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id").isin([21, 35, 36]))
        .select("id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("attr_key").isNotNull())
    .withColumn("value_2_upper", F.upper("value_2"))
)

cleaned = (
    base
    .filter(F.col("attr_key") != "gift")
    .withColumn("value_2_filled", F.coalesce("value_2", F.lit("N/A")))
)

kept_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.to_date("created_at") >= "2025-09-01")
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.size(F.collect_set("value_2")).alias("num_value_2s"),
        F.count(F.when(F.col("value_2") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count(F.when(F.col("value_2") == "BUNDLE", 1)).alias("bundle_rows"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .orderBy(F.col("welcome5_rows").asc_nulls_last())
)
