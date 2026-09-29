from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.qty"),
        F.col("item.price"),
    )
)

valid = (
    exploded
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        (F.col("price") * F.col("qty")).alias("price_times_qty"),
        (F.col("qty") - F.col("price")).alias("qty_minus_price"),
    )
)

valid_carts = (
    valid
    .filter(F.col("price") > 90)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        F.to_date("created_at").alias("created_at_date"),
    )
)

kept_keys = (
    valid_carts
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("created_at") < "2024-12-13")
        .select("id")
    )
)

reconciled = (
    valid_carts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    reconciled
    .groupBy(F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"))
    .agg(
        F.min("qty").alias("min_qty"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
)

result = daily.select("qty_tier", "min_qty", "distinct_ids", "avg_price")
