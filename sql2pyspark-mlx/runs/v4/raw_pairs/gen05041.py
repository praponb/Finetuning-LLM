from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id") == 8)
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    flattened_attrs
    .withColumn("max_item_price_tier", F.when(F.col("max_item_price") >= 189, "HIGH").otherwise("LOW"))
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .groupingSets(
        [["max_item_price_tier", "hour_window"], ["max_item_price_tier"]],
        "max_item_price_tier",
        "hour_window",
    )
    .agg(
        F.percentile_approx("max_item_price", [0.25, 0.5, 0.75]).alias("max_item_price_quartiles"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.max("max_item_price").alias("max_max_item_price"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("max_max_item_price") > 0)
    .orderBy(F.col("max_max_item_price").asc_nulls_last())
)
