from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("unknown")).alias("gift_or_default"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.min("created_at").alias("started_at"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.max("created_at").alias("ended_at"),
    )
    .filter(F.col("distinct_ids_2") < 1000)
)
