from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("code")
w_running = Window.partitionBy("code").orderBy(F.col("is_vip").desc_nulls_first())
w3 = Window.orderBy("is_vip")
w4 = Window.partitionBy("code").orderBy("is_vip_cume_dist")

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

lines_carts = lines.select("attr_value", F.explode("coupon_codes").alias("code"))

valid = (
    lines_carts
    .filter(F.col("code").isNotNull())
    .select(
        "*",
        F.when(F.col("code") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.coalesce("code", F.lit("N/A")).alias("code_clean"),
    )
)

enriched = (
    valid
    .select(
        "attr_value",
        "code",
        F.max("is_vip").over(w_part).alias("code_max_is_vip"),
        F.percent_rank().over(w_running).alias("is_vip_percent_rank"),
        F.cume_dist().over(w3).alias("is_vip_cume_dist"),
        F.count("is_vip").over(w_part).alias("code_count_is_vip"),
        F.stddev_samp("is_vip").over(w_part).alias("code_stddev_samp_is_vip"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.round((F.col("code_count_is_vip") / F.sum("code_count_is_vip").over(w_part)), 4).alias("code_count_is_vip_share"),
        F.avg("code_stddev_samp_is_vip").over(w_part).alias("code_avg_code_stddev_samp_is_vip"),
        F.percent_rank().over(w4).alias("is_vip_cume_dist_percent_rank"),
    )
)

metrics_carts = (
    metrics
    .select(
        "code",
        "code_max_is_vip",
        "is_vip_percent_rank",
        "code_stddev_samp_is_vip",
        "code_count_is_vip_share",
        "is_vip_cume_dist_percent_rank",
        F.round((F.col("code_count_is_vip") / F.sum("code_count_is_vip").over(w_part)), 4).alias("code_count_is_vip_share_2"),
    )
)

enriched_carts = (
    metrics_carts
    .filter(F.col("code_count_is_vip_share_2") < 0.2)
    .select(
        "*",
        F.stddev_samp("code_count_is_vip_share_2").over(w_part).alias("code_stddev_samp_code_count_is_vip_share_2"),
    )
)

result = (
    enriched_carts
    .select(
        "code",
        "code_max_is_vip",
        "code_count_is_vip_share",
        "code_count_is_vip_share_2",
        "code_stddev_samp_code_count_is_vip_share_2",
    )
    .orderBy(
        "code_count_is_vip_share",
        F.col("code_stddev_samp_code_count_is_vip_share_2").desc(),
    )
)
