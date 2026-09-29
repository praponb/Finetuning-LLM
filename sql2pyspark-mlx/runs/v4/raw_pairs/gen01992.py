from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    lines
    .rollup(
        "product_id",
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.count_if(F.col("promo") == "VIP").alias("vip_events"),
        F.min("created_at").alias("started_at"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
        F.countDistinct("promo").alias("distinct_promos"),
        F.countDistinct("promo").alias("distinct_promos_2"),
    )
)
