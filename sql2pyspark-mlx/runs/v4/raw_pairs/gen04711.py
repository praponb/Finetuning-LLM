from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.hour("created_at").between(7, 12))
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("ended_at"),
        F.max("cart_value").alias("max_cart_value"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.percentile_approx("cart_value", 0.95).alias("cart_value_p95"),
    )
    .filter(F.col("max_cart_value") >= 0)
)
