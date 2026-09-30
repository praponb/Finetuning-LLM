from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("tag", "customer_id")
w2 = Window.orderBy(F.col("price").desc())
w_hist = Window.orderBy(F.col("qty").asc_nulls_last())
w4 = Window.partitionBy("tag")

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

lines_carts = (
    lines
    .select("id", "customer_id", "qty", "price", F.explode("coupon_codes").alias("tag"))
)

remaining_keys = (
    lines_carts
    .select("id", "customer_id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "BUNDLE"))
        .select("id", "customer_id")
    )
)

verified = (
    lines_carts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("tag_count_price", F.count("price").over(w_part))
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w_part)), 4))
    .withColumn("price_dense_rank", F.dense_rank().over(w2))
    .withColumn("qty_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("qty_share_2", F.round((F.col("qty") / F.sum("qty").over(w4)), 4))
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("qty_share_2") >= 0.87, "PREMIUM").otherwise("STANDARD").alias("qty_share_2_tier"),
    )
    .agg(
        F.percentile_approx("qty_share", 0.9).alias("qty_share_p90"),
        F.round(F.sum("qty") / F.nullif(F.count(F.when(F.col("tag") == "VIP", 1)), F.lit(0)), 2).alias("qty_per_vip"),
    )
)

result = (
    stats
    .select("qty_share_2_tier", "qty_per_vip")
    .orderBy(F.col("qty_share_2_tier").desc())
)
