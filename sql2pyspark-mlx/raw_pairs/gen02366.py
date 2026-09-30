from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("k")
w2 = Window.partitionBy("k").orderBy("k_avg_coupon_codes_count")

lines = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

remaining_keys = (
    lines
    .select("id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "BUNDLE"))
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.size("coupon_codes") > 1)
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.avg("coupon_codes_count").over(w_grp).alias("k_avg_coupon_codes_count"),
    )
)

cleaned_carts = (
    with_windows
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "k",
        "attr_value",
        "k_avg_coupon_codes_count",
        F.round("k_avg_coupon_codes_count", 0).alias("k_avg_coupon_codes_count_rounded"),
    )
)

base = (
    cleaned_carts
    .filter(F.col("k_avg_coupon_codes_count_rounded").isNotNull())
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("NONE")))
)

scored = (
    base
    .select(
        "*",
        F.dense_rank().over(w2).alias("k_avg_coupon_codes_count_dense_rank"),
        F.min("k_avg_coupon_codes_count").over(w_grp).alias("k_min_k_avg_coupon_codes_count"),
    )
)

result = (
    scored
    .groupBy("k")
    .agg(
        F.count(F.when(F.col("k") == "weight_kg", 1)).alias("weight_kg_count"),
        F.countDistinct("k").alias("distinct_ks"),
        F.percentile_approx("k_avg_coupon_codes_count_rounded", 0.5).alias("k_avg_coupon_codes_count_rounded_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("coupon_codes_count"), 2).alias("avg_coupon_codes_count"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_count"),
    )
    .filter(F.col("utm_source_count") >= 1)
    .orderBy(F.col("distinct_ks").asc_nulls_last())
)
