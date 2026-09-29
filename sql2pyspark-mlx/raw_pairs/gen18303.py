from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element")
w2 = Window.partitionBy(F.to_date("created_at"), "promo").orderBy("product_id")
w3 = Window.orderBy(F.col("qty").asc_nulls_first())
w4 = Window.partitionBy("promo", "element").orderBy("qty_percent_rank")

exploded = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

lines = (
    exploded
    .select(
        "created_at",
        "product_id",
        "qty",
        "promo",
        F.explode("coupon_codes").alias("element"),
    )
)

enriched = (
    lines
    .select(
        "qty",
        "promo",
        "element",
        F.avg("qty").over(w).alias("element_avg_qty"),
        F.cume_dist().over(w2).alias("product_id_cume_dist"),
        F.percent_rank().over(w3).alias("qty_percent_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("element").isNotNull())
    .select(
        "promo",
        "element",
        "element_avg_qty",
        "qty_percent_rank",
        F.concat("promo", F.lit(", "), "element").alias("promo_element"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

metrics = (
    filtered
    .select(
        "element",
        "element_avg_qty",
        "qty_percent_rank",
        "promo_element",
        "promo_clean",
        F.dense_rank().over(w4).alias("qty_percent_rank_dense_rank"),
    )
)

summary = (
    metrics
    .cube(
        "element",
        F.when(F.col("qty_percent_rank") >= 0.74, "LARGE").otherwise("SMALL").alias("qty_percent_rank_tier"),
    )
    .agg(
        F.min("element_avg_qty").alias("min_element_avg_qty"),
        F.round(F.sum("qty_percent_rank_dense_rank") / F.nullif(F.count(F.when(F.col("element") == "BUNDLE", 1)), F.lit(0)), 2).alias("qty_percent_rank_dense_rank_per_bundle"),
        F.count_if(F.col("element") == "VIP").alias("vip_events"),
    )
)

result = (
    summary
    .select("element", "min_element_avg_qty", "vip_events")
    .orderBy("vip_events")
)
