from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")
w2 = Window.partitionBy("key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

exploded = flattened.select("id", "key", F.explode("coupon_codes").alias("tag"))

cleaned = (
    exploded
    .filter(F.col("tag").isNotNull() & (F.col("key") == "gift"))
    .select(
        "*",
        F.when(F.col("tag") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.substring("tag", 1, 3).alias("tag_prefix"),
    )
)

enriched = (
    cleaned
    .filter(F.col("tag").isin(["WELCOME5", "BUNDLE", "VIP"]))
    .select(
        "key",
        "tag",
        "is_vip",
        F.sum("is_vip").over(w).alias("tag_sum_is_vip"),
        F.stddev_samp("is_vip").over(w2).alias("key_stddev_samp_is_vip"),
    )
)

stats = (
    enriched
    .groupBy("key")
    .agg(
        F.percentile_approx("tag_sum_is_vip", [0.25, 0.5, 0.75]).alias("tag_sum_is_vip_quartiles"),
        F.max(F.when(F.col("key") == "gift", F.col("tag_sum_is_vip"))).alias("gift_max_tag_sum_is_vip"),
        F.count_if(F.col("tag") == "SPRING10").alias("spring10_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .filter(F.col("gift_max_tag_sum_is_vip") > 0)
)

result = (
    stats
    .select(
        "key",
        "tag_sum_is_vip_quartiles",
        "gift_max_tag_sum_is_vip",
        "spring10_events",
        "key_list",
        F.when(F.col("key").rlike("^(device|utm_source)$"), "CORE").when(F.col("key").rlike("^(gift|weight_kg)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("key_zone"),
    )
    .orderBy(F.col("gift_max_tag_sum_is_vip").desc())
)
