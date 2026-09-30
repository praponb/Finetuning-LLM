from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("ship_to.city").alias("ship_city"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.col("attributes").getItem("gift").alias("gift"),
    )
)

result = (
    flattened_attrs
    .groupBy("ship_city")
    .agg(
        F.count_if(F.col("ship_city") == "Chiang Mai").alias("chiang_mai_events"),
        F.min("created_at").alias("first_created_at"),
        F.count(F.when(F.col("ship_city") == "New York", 1)).alias("new_york_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.min("created_at").alias("first_created_at_2"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy(F.col("new_york_rows").asc_nulls_first())
)
