from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
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

base = (
    lines
    .filter(F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 22:00:00")))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        "promo",
        F.col("product_id").cast("double").alias("product_id_str"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
    )
)

valid = (
    base
    .filter(F.col("promo").isNotNull() & (~F.col("promo").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("least_product_id_str_qty", F.least("product_id_str", "qty"))
)

filtered = (
    valid
    .filter(F.col("qty") == 4)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        "product_id_str",
        "promo_filled",
        "least_product_id_str_qty",
        (F.datediff(F.current_date(), "created_at") / 365).alias("years_since_created_at"),
        (F.col("qty") - F.col("product_id")).alias("qty_minus_product_id"),
    )
)

result = (
    filtered
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .withColumn("qty_tier", F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["activity_day", "qty_tier"], ["activity_day"], []],
        "activity_day",
        "qty_tier",
    )
    .agg(
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.round(F.avg("least_product_id_str_qty"), 2).alias("avg_least_product_id_str_qty"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("span_minutes").asc_nulls_last())
)
