from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("code")
w3 = Window.partitionBy("promo").orderBy(F.col("promo_stddev_samp_qty").desc())
w4 = Window.partitionBy("code").orderBy(F.col("product_id").asc_nulls_last())
w5 = Window.partitionBy("code").orderBy(F.col("product_id_percent_rank").desc_nulls_last())

flattened = df_carts.select("id", "coupon_codes", F.inline("items"))

flattened_carts = (
    flattened
    .select(
        "id",
        "product_id",
        "qty",
        "promo",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

metrics = (
    flattened_carts
    .filter(F.col("product_id") != 8)
    .select(
        "*",
        F.stddev_samp("qty").over(w).alias("promo_stddev_samp_qty"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w3).alias("promo_stddev_samp_qty_percent_rank"),
        F.percent_rank().over(w4).alias("product_id_percent_rank"),
        F.avg("product_id").over(w2).alias("code_avg_product_id"),
    )
)

valid = (
    scored
    .filter((~F.col("code").eqNullSafe(F.lit("BUNDLE"))) & (F.col("product_id") < 9))
    .select(
        "id",
        "product_id",
        "qty",
        "code",
        "promo_stddev_samp_qty",
        "product_id_share",
        "promo_stddev_samp_qty_percent_rank",
        "product_id_percent_rank",
        "code_avg_product_id",
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
        F.coalesce("code", F.lit("DIRECT")).alias("resolved_code"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.max("product_id_percent_rank").over(w2).alias("code_max_product_id_percent_rank"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.cume_dist().over(w5).alias("product_id_percent_rank_cume_dist"),
    )
)

report = (
    with_windows
    .select(
        "id",
        "product_id",
        "code",
        "promo_stddev_samp_qty",
        "product_id_share",
        "product_id_percent_rank",
        "code_avg_product_id",
        "promo_filled",
        "resolved_code",
        F.when(F.col("code").rlike("^(WELCOME5|BUNDLE)$"), "OTHER_ZONE").when(F.col("code").rlike("^(SPRING10|VIP)$"), "PRIORITY").otherwise("EXTENDED").alias("code_zone"),
    )
    .orderBy(F.col("product_id_share").desc_nulls_last())
)

result = (
    report
    .select(
        "id",
        "product_id",
        "code",
        "promo_filled",
        "resolved_code",
        "code_zone",
        F.when(F.col("code_zone").rlike("^(PRIORITY|OTHER_ZONE)$"), "GROUP_A").otherwise("EXTENDED").alias("code_zone_zone"),
    )
)
