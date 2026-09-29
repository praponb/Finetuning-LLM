from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list_2"),
        F.count_if(F.col("ship_city") == "Singapore").alias("singapore_events"),
    )
    .orderBy("customer_id")
)
