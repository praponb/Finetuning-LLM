from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy(F.to_date("created_at")).orderBy(F.col("coupon_codes_count").desc_nulls_first())

lines = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

cleaned = (
    lines
    .filter(F.col("promo").isNotNull() & (F.col("created_at") > F.lit("2025-03-01").cast("date")))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        F.size("coupon_codes").alias("coupon_codes_count"),
    )
)

with_windows = (
    cleaned
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "id",
        "created_at",
        "price",
        "coupon_codes_count",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.percent_rank().over(w2).alias("coupon_codes_count_percent_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("coupon_codes_count_percent_rank") > 0.76)
    .select(
        "price",
        "coupon_codes_count",
        "price_share",
        "coupon_codes_count_percent_rank",
        F.when(F.col("coupon_codes_count") >= 63, "High").when(F.col("coupon_codes_count") >= 26, "Medium").otherwise("Low").alias("coupon_codes_count_tier"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("price_share") >= 0.72, "HIGH").otherwise("LOW").alias("price_share_tier"),
    )
    .agg(
        F.percentile_approx("coupon_codes_count", 0.9).alias("coupon_codes_count_p90"),
        F.percentile_approx("price_share", 0.5).alias("price_share_p50"),
        F.min("price").alias("min_price"),
        F.percentile_approx("price_share", [0.25, 0.5, 0.75]).alias("price_share_quartiles"),
    )
    .orderBy(F.col("price_share_tier").desc_nulls_last())
)
