from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.to_date("created_at") < "2024-11-04")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    parsed
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .cube("hour_window", "activity_day")
    .agg(
        F.min("created_at").alias("first_seen"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price_2"),
        F.percentile_approx("max_item_price", [0.25, 0.5, 0.75]).alias("max_item_price_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("hour_window").desc())
)
