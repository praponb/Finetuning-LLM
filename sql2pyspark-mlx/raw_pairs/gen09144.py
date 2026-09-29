from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id") == 25)
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
    )
)

result = (
    flattened_attrs
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.count("*").alias("row_count"),
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50"),
    )
    .orderBy(F.col("bulk_lines_p50").asc_nulls_first())
)
