from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy(F.col("discount_pct").desc())
w3 = Window.orderBy("discount_pct", "id")

valid = (
    df_promotions
    .filter(F.col("promo_code").isin(["SPRING10", "WELCOME5", "BUNDLE"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.upper("promo_code").alias("promo_code_upper"),
        F.substring("promo_code", 1, 2).alias("promo_code_prefix"),
    )
)

with_windows = (
    valid
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w2))
    .withColumn("discount_pct_bucket", F.ntile(4).over(w3))
)

base = (
    with_windows
    .filter((F.col("ends_on") >= F.lit("2024-01-12").cast("date")) & F.col("discount_pct").between(0.13, 0.28))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "promo_code_prefix",
        "discount_pct_share",
        "discount_pct_cume_dist",
        "discount_pct_bucket",
        F.when(F.col("channel").isin(["mobile", "store"]), "Priority").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

totals = (
    base
    .groupBy(
        F.when(F.col("discount_pct_bucket") >= 7, "HIGH").otherwise("LOW").alias("discount_pct_bucket_tier"),
    )
    .agg(
        F.round(F.sum("discount_pct_bucket") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_bucket_per_welcome5"),
        F.min("discount_pct").alias("min_discount_pct"),
        F.sum(F.when(F.col("promo_code") == "VIP", F.col("discount_pct_cume_dist")).otherwise(0)).alias("vip_discount_pct_cume_dist"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount_pct_share")).otherwise(0)).alias("store_discount_pct_share"),
    )
    .filter(F.col("vip_discount_pct_cume_dist") > 0)
)

result = (
    totals
    .select("discount_pct_bucket_tier", "min_discount_pct", "store_discount_pct_share")
    .orderBy(
        F.col("store_discount_pct_share").desc_nulls_first(),
        "discount_pct_bucket_tier",
    )
)
