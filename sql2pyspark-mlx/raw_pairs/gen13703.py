from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.orderBy("is_bundle")
w3 = Window.orderBy(F.col("is_bundle").desc())

exploded = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

exploded_carts = exploded.select("value", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    exploded_carts
    .filter(~F.col("code").eqNullSafe(F.lit("BUNDLE")))
    .withColumn("code_lower", F.lower("code"))
)

filtered_carts = (
    filtered
    .filter(F.col("code").isNotNull() & F.col("code").isin(["VIP", "WELCOME5", "SPRING10"]))
    .select(
        "*",
        F.when(F.col("code") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.coalesce("code", F.lit("DIRECT")).alias("code_filled"),
    )
)

enriched = (
    filtered_carts
    .withColumn("code_stddev_samp_is_bundle", F.stddev_samp("is_bundle").over(w))
    .withColumn("is_bundle_cume_dist", F.cume_dist().over(w2))
    .withColumn("code_count_is_bundle", F.count("is_bundle").over(w))
    .withColumn("is_bundle_rank", F.rank().over(w3))
)

result = (
    enriched
    .withColumn("is_bundle_cume_dist_tier", F.when(F.col("is_bundle_cume_dist") >= 0.75, "PREMIUM").otherwise("STANDARD"))
    .withColumn("code_count_is_bundle_tier", F.when(F.col("code_count_is_bundle") >= 14, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["is_bundle_cume_dist_tier", "code_count_is_bundle_tier"], ["is_bundle_cume_dist_tier"]],
        "is_bundle_cume_dist_tier",
        "code_count_is_bundle_tier",
    )
    .agg(
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.round(F.sum("is_bundle") / F.nullif(F.count(F.when(F.col("code") == "WELCOME5", 1)), F.lit(0)), 2).alias("is_bundle_per_welcome5"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.countDistinct("code").alias("distinct_codes"),
        F.countDistinct("code").alias("distinct_codes_2"),
        F.grouping("is_bundle_cume_dist_tier").alias("gid"),
    )
)
