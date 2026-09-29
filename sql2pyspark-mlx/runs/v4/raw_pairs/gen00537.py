from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.hour("created_at").between(11, 17))
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    flattened_attrs
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .groupingSets(
        [["activity_day", "hour_window"], ["activity_day"], []],
        "activity_day",
        "hour_window",
    )
    .agg(
        F.countDistinct("country_or_unknown").alias("distinct_country_or_unknowns"),
        F.max("created_at").alias("last_seen"),
        F.max("created_at").alias("last_seen_2"),
        F.min("created_at").alias("first_seen"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("hour_window").desc())
)
