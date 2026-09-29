from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy(F.col("tag_length").desc_nulls_last())
w2 = Window.partitionBy("tag")
w3 = Window.orderBy(F.col("tag_length").desc_nulls_first())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

lines_carts = lines.select("id", "value", F.explode_outer("coupon_codes").alias("tag"))

valid = (
    lines_carts
    .filter(F.col("tag") == "SPRING10")
    .select("*", F.length("tag").alias("tag_length"))
)

base = (
    valid
    .filter(F.col("tag").isNotNull())
    .select("*", F.coalesce("tag", F.lit("unassigned")).alias("tag_filled"))
)

enriched = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("tag_length_dense_rank"),
        F.sum("tag_length").over(w2).alias("tag_sum_tag_length"),
        F.rank().over(w3).alias("tag_length_rank"),
    )
)

matched_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id") == 22)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_carts = (
    reconciled
    .filter(F.col("tag").isNotNull())
    .select(
        "value",
        "tag",
        "tag_filled",
        "tag_length_dense_rank",
        "tag_sum_tag_length",
        "tag_length_rank",
        F.length("tag").alias("tag_length_2"),
    )
)

summary = (
    valid_carts
    .groupBy("tag")
    .agg(
        F.round(F.sum("tag_length_dense_rank") / F.nullif(F.count(F.when(F.col("tag") == "BUNDLE", 1)), F.lit(0)), 2).alias("tag_length_dense_rank_per_bundle"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count"),
    )
    .filter(F.col("tag_length_dense_rank_per_bundle") > 0)
)

result = (
    summary
    .select("tag", "tag_length_dense_rank_per_bundle", "vip_count")
    .orderBy(F.col("tag_length_dense_rank_per_bundle").desc())
)
