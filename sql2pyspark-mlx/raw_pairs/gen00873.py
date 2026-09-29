from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy(F.col("tag_length").desc())
w2 = Window.orderBy(F.col("tag_length").desc_nulls_last())
w3 = Window.orderBy(F.col("tag_length_percent_rank").desc())
w4 = Window.orderBy(F.col("tag_length_percent_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("tag_length_rank").desc())
w6 = Window.orderBy(F.col("tag_length").asc_nulls_first())

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

valid = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-01 11:00:00").cast("timestamp")) & F.col("tag").isin(["VIP", "BUNDLE"]))
    .withColumn("tag_length", F.length("tag"))
    .withColumn("tag_group", F.when(F.col("tag").isin(["VIP", "BUNDLE"]), "Core").otherwise("Other"))
)

scored = (
    valid
    .select(
        "id",
        "tag_length",
        "tag_group",
        F.percent_rank().over(w).alias("tag_length_percent_rank"),
        F.rank().over(w2).alias("tag_length_rank"),
    )
)

enriched = (
    scored
    .filter(F.col("tag_length") >= 54)
    .select("*", F.dense_rank().over(w3).alias("tag_length_percent_rank_dense_rank"))
)

scored_carts = (
    enriched
    .select(
        "tag_length_percent_rank",
        "tag_length_rank",
        "tag_length_percent_rank_dense_rank",
        F.rank().over(w4).alias("tag_length_percent_rank_rank"),
        F.percent_rank().over(w5).alias("tag_length_rank_percent_rank"),
        F.rank().over(w6).alias("tag_length_rank_2"),
        F.rank().over(w2).alias("tag_length_rank_3"),
    )
)

with_windows = (
    scored_carts
    .select(
        "tag_length_rank_percent_rank",
        "tag_length_rank_3",
        F.percent_rank().over(w3).alias("tag_length_percent_rank_percent_rank"),
    )
)

daily = (
    with_windows
    .withColumn("tag_length_rank_percent_rank_tier", F.when(F.col("tag_length_rank_percent_rank") >= 0.61, "PREMIUM").otherwise("STANDARD"))
    .cube("tag_length_rank_percent_rank_tier")
    .agg(
        F.percentile_approx("tag_length_rank_3", [0.25, 0.5, 0.75]).alias("tag_length_rank_3_quartiles"),
        F.percentile_approx("tag_length_percent_rank_percent_rank", 0.5).alias("tag_length_percent_rank_percent_rank_p50"),
        F.percentile_approx("tag_length_rank_percent_rank", 0.5).alias("tag_length_rank_percent_rank_p50"),
        F.round(F.avg("tag_length_percent_rank_percent_rank"), 2).alias("avg_tag_length_percent_rank_percent_rank"),
        F.grouping("tag_length_rank_percent_rank_tier").alias("gid"),
    )
)

result = (
    daily
    .select(
        "tag_length_rank_3_quartiles",
        "tag_length_rank_percent_rank_p50",
        "avg_tag_length_percent_rank_percent_rank",
        "gid",
    )
)
