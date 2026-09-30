from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("promo")
w2 = Window.partitionBy("element", "promo").orderBy(F.col("is_vip").desc_nulls_last())
w3 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_ordered = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_hist = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

exploded_carts = exploded.select("created_at", "promo", F.explode("coupon_codes").alias("element"))

cleaned = (
    exploded_carts
    .filter(F.col("promo").isin(["SPRING10", "VIP", "BUNDLE"]))
    .select(
        "*",
        F.when(F.col("element") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.lower("element").alias("element_lower"),
    )
)

enriched = (
    cleaned
    .withColumn("promo_stddev_samp_is_vip", F.stddev_samp("is_vip").over(w_running))
    .withColumn("promo_min_is_vip", F.min("is_vip").over(w_running))
    .withColumn("is_vip_percent_rank", F.percent_rank().over(w2))
)

base = (
    enriched
    .filter(F.col("is_vip") <= 42)
    .select(
        "created_at",
        "promo",
        "element_lower",
        "promo_stddev_samp_is_vip",
        "promo_min_is_vip",
        "is_vip_percent_rank",
        F.coalesce("element", F.lit("N/A")).alias("element_filled"),
        F.coalesce("promo", F.lit("NONE")).alias("resolved_promo"),
    )
)

cleaned_carts = (
    base
    .filter(F.hour("created_at").between(7, 14) & (~F.col("promo").eqNullSafe(F.lit("VIP"))))
    .select(
        "created_at",
        "promo",
        "element_lower",
        "promo_stddev_samp_is_vip",
        "promo_min_is_vip",
        "is_vip_percent_rank",
        "element_filled",
        "resolved_promo",
        F.year("created_at").alias("created_at_year"),
    )
)

enriched_carts = (
    cleaned_carts
    .withColumn("is_vip_percent_rank_sum_30m", F.sum("is_vip_percent_rank").over(w3))
    .withColumn("promo_min_is_vip_max_1h", F.max("promo_min_is_vip").over(w_ordered))
    .withColumn("promo_max_promo_min_is_vip", F.max("promo_min_is_vip").over(w_running))
    .withColumn("promo_min_is_vip_max_1h_2", F.max("promo_min_is_vip").over(w_ordered))
    .withColumn("created_at_year_count_15m", F.count("created_at_year").over(w_hist))
)

result = (
    enriched_carts
    .select(
        "created_at",
        "promo_min_is_vip",
        "element_filled",
        "resolved_promo",
        "created_at_year",
        "promo_min_is_vip_max_1h",
        "promo_max_promo_min_is_vip",
        "promo_min_is_vip_max_1h_2",
        "created_at_year_count_15m",
    )
    .orderBy(F.col("created_at").desc_nulls_last())
)
