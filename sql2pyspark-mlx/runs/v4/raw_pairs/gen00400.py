from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    unpacked
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["activity_day", "customer_id"], ["activity_day"], []],
        "activity_day",
        "customer_id",
    )
    .agg(
        F.max("created_at").alias("last_seen"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
