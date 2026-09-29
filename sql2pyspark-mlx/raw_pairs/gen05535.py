from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("k")
w2 = Window.partitionBy("k").orderBy(F.col("k_length").desc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("k_length").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "k",
        "attr_value",
        F.length("k").alias("k_length"),
        F.when(F.col("k") == "utm_source", 1).otherwise(0).alias("is_utm_source"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .intersect(
        df_carts
        .filter(F.to_date("created_at") < "2025-07-06")
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "attr_value",
        "resolved_k",
        F.min("k_length").over(w_running).alias("k_min_k_length"),
        F.dense_rank().over(w2).alias("k_length_dense_rank"),
        F.dense_rank().over(w3).alias("k_length_dense_rank_2"),
    )
)

filtered_carts = (
    scored
    .filter(F.col("k_length_dense_rank_2") > 4)
    .select(
        "*",
        F.coalesce("k_length_dense_rank", F.lit(0)).alias("k_length_dense_rank_filled"),
    )
)

stats = (
    filtered_carts
    .cube(
        F.when(F.col("k_min_k_length") >= 79, "HIGH").otherwise("LOW").alias("k_min_k_length_tier"),
        F.when(F.col("k_length_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("k_length_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("k_length_dense_rank"), 2).alias("avg_k_length_dense_rank"),
        F.round(F.avg("k_length_dense_rank_filled"), 2).alias("avg_k_length_dense_rank_filled"),
        F.round(F.avg("k_length_dense_rank_2"), 2).alias("avg_k_length_dense_rank_2"),
        F.round(F.avg("k_min_k_length"), 2).alias("avg_k_min_k_length"),
        F.min("k_min_k_length").alias("min_k_min_k_length"),
        F.round(F.avg("k_length_dense_rank_2"), 2).alias("avg_k_length_dense_rank_2_2"),
    )
    .filter(F.col("avg_k_length_dense_rank_2") < 1000000)
)

result = (
    stats
    .select(
        "k_min_k_length_tier",
        "k_length_dense_rank_tier",
        "avg_k_length_dense_rank_2",
        "avg_k_min_k_length",
        "min_k_min_k_length",
    )
)
