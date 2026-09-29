from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")

exploded = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

lines = exploded.select("qty", "price", "promo", F.explode("coupon_codes").alias("code"))

base = (
    lines
    .filter(~F.col("code").isin(["SPRING10", "WELCOME5", "BUNDLE"]))
    .select(
        "*",
        F.coalesce("qty", F.lit(0)).alias("qty_filled"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("code_filled"),
    )
)

metrics = (
    base
    .select(
        "price",
        "promo",
        "qty_filled",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
    )
)

result = (
    metrics
    .withColumn("price_tier", F.when(F.col("price") >= 264, "HIGH").otherwise("LOW"))
    .withColumn("qty_filled_tier", F.when(F.col("qty_filled") >= 61, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["price_tier", "qty_filled_tier"], ["price_tier"]],
        "price_tier",
        "qty_filled_tier",
    )
    .agg(
        F.countDistinct("promo").alias("distinct_promos"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.percentile_approx("qty_share", 0.95).alias("qty_share_p95"),
    )
    .orderBy("qty_share_p95")
)
