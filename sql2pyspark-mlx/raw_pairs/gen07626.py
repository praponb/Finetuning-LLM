from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_first())
w4 = Window.partitionBy("promo").orderBy(F.col("qty").desc())
w5 = Window.partitionBy(F.year("created_at"), "promo")
w6 = Window.partitionBy("promo").orderBy(F.col("product_id").asc_nulls_last())
w7 = Window.partitionBy("promo").orderBy(F.col("qty_sum_1h").desc_nulls_last())
w8 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w9 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = df_carts.select("id", "created_at", F.inline("items"))

with_windows = (
    flattened
    .filter(F.col("product_id") <= 8)
    .withColumn("promo_stddev_samp_price", F.stddev_samp("price").over(w))
    .withColumn("qty_sum_1h", F.sum("qty").over(w2))
    .withColumn("qty_dense_rank", F.dense_rank().over(w3))
)

scored = (
    with_windows
    .select(
        "*",
        F.rank().over(w4).alias("qty_rank"),
        F.round((F.col("promo_stddev_samp_price") / F.sum("promo_stddev_samp_price").over(w5)), 4).alias("promo_stddev_samp_price_share"),
    )
)

base = (
    scored
    .filter(F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 22:00:00")))
    .select(
        "*",
        F.round((F.col("product_id") / F.col("qty_sum_1h")), 2).alias("product_id_per_qty_sum_1h"),
        F.coalesce("promo", F.lit("NONE")).alias("promo_filled"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.rank().over(w6).alias("product_id_rank"),
        F.cume_dist().over(w7).alias("qty_sum_1h_cume_dist"),
        F.max("product_id").over(w8).alias("product_id_max_2h"),
    )
)

metrics = enriched.withColumn("qty_dense_rank_count_1h", F.count("qty_dense_rank").over(w9))

kept_keys = (
    metrics
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.hour("created_at").between(8, 16))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    verified
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.round(F.avg("qty_dense_rank"), 2).alias("avg_qty_dense_rank"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.sum(F.when(F.col("promo") == "VIP", F.col("product_id_rank"))).alias("vip_total_product_id_rank"),
    )
)

result = (
    daily
    .select(
        "activity_day",
        "avg_qty_dense_rank",
        "duration_min",
        "avg_qty",
        "vip_total_product_id_rank",
    )
    .orderBy(F.col("duration_min").desc_nulls_first())
)
