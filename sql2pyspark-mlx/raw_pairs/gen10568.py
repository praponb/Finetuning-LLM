from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("elem.product_id"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("day", "created_at"))
    .withColumn("product_id_tier", F.when(F.col("product_id") >= 9, "LARGE").otherwise("SMALL"))
    .withColumn("hour_window", F.date_trunc("hour", "created_at"))
    .rollup("activity_day", "product_id_tier", "hour_window")
    .agg(
        F.max(F.when(F.col("promo") == "SPRING10", F.col("product_id"))).alias("spring10_max_product_id"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.grouping("activity_day").alias("grouping_id"),
    )
    .orderBy("hour_window")
)
