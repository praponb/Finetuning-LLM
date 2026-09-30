from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag").orderBy("qty")
w2 = Window.orderBy(F.col("qty").desc_nulls_last())
w3 = Window.partitionBy("tag")
w4 = Window.partitionBy("tag").orderBy(F.col("qty").desc())
w5 = Window.orderBy(F.col("qty_rank_2").desc_nulls_last())
w_ordered = Window.partitionBy("tag").orderBy(F.col("qty_rank").desc_nulls_last())
w_part = Window.partitionBy("tag").orderBy("qty_rank")
w8 = Window.orderBy(F.col("qty_rank").desc_nulls_last())
w9 = Window.orderBy(F.col("qty_share").desc_nulls_first())

flattened = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.promo"),
    )
)

flattened_carts = flattened.select("qty", F.explode("coupon_codes").alias("tag"))

base = (
    flattened_carts
    .filter(F.col("tag").eqNullSafe(F.lit("WELCOME5")))
    .withColumn("tag_upper", F.upper("tag"))
)

metrics = (
    base
    .filter(F.col("qty") == 3)
    .select(
        "qty",
        "tag",
        F.rank().over(w).alias("qty_rank"),
        F.rank().over(w2).alias("qty_rank_2"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share"),
        F.count("qty").over(w3).alias("tag_count_qty"),
        F.dense_rank().over(w4).alias("qty_dense_rank"),
    )
)

enriched = (
    metrics
    .withColumn("qty_rank_2_cume_dist", F.cume_dist().over(w5))
    .withColumn("qty_rank_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("qty_rank_dense_rank", F.dense_rank().over(w_part))
    .withColumn("qty_rank_rank", F.rank().over(w8))
    .withColumn("qty_share_percent_rank", F.percent_rank().over(w9))
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("tag_count_qty") >= 14, "HIGH").otherwise("LOW").alias("tag_count_qty_tier"),
        F.when(F.col("qty_rank_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("qty_rank_rank_tier"),
        "tag",
    )
    .agg(
        F.percentile_approx("qty_share", [0.25, 0.5, 0.75]).alias("qty_share_quartiles"),
        F.round(F.avg("qty_share_percent_rank"), 2).alias("avg_qty_share_percent_rank"),
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_count"),
    )
)

result = (
    stats
    .select(
        "tag_count_qty_tier",
        "qty_rank_rank_tier",
        "tag",
        "qty_share_quartiles",
        "avg_qty_share_percent_rank",
    )
    .orderBy(
        F.col("tag_count_qty_tier").desc_nulls_last(),
        F.col("qty_rank_rank_tier").desc(),
    )
)
