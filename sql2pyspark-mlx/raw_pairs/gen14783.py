from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("key").orderBy(F.col("coupon_codes_count").desc_nulls_first())
w3 = Window.partitionBy("key", "customer_id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("coupon_codes_count").asc_nulls_first())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

cleaned = (
    lines
    .filter(~F.col("key").isin(["gift", "utm_source"]))
    .withColumn("key_prefix", F.substring("key", 1, 2))
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
    .withColumn("key_clean", F.coalesce("key", F.lit("OTHER")))
)

matched_keys = (
    cleaned
    .select("id", "customer_id")
    .intersectAll(
        df_carts
        .filter(F.col("created_at") < "2025-07-01")
        .select("id", "customer_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.hour("created_at").between(12, 18))
        .select("id", "customer_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "key",
        "value",
        "key_prefix",
        "key_clean",
        F.sum("coupon_codes_count").over(w).alias("customer_id_sum_coupon_codes_count"),
        F.rank().over(w2).alias("coupon_codes_count_rank"),
        F.max("coupon_codes_count").over(w3).alias("key_max_coupon_codes_count"),
        F.rank().over(w4).alias("coupon_codes_count_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "key",
        "value",
        "key_prefix",
        "key_clean",
        "customer_id_sum_coupon_codes_count",
        "coupon_codes_count_rank",
        "key_max_coupon_codes_count",
        "coupon_codes_count_rank_2",
        F.when(F.col("key").rlike("^(utm_source|gift)$"), "GROUP_B").when(F.col("key").rlike("^(device|weight_kg)$"), "CORE").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(
        F.col("coupon_codes_count_rank").desc(),
        F.col("coupon_codes_count_rank_2").desc(),
    )
)
