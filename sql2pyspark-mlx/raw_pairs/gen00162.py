from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("entry.product_id"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("pos") >= 3, "LARGE").otherwise("SMALL").alias("pos_tier"),
        F.when(F.col("price") >= 236, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.percentile_approx("pos", [0.25, 0.5, 0.75]).alias("pos_quartiles"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_count"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("bundle_count")
)
