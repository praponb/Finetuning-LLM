from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "attributes",
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.min("created_at").alias("first_seen"),
        F.count("*").alias("row_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min_2"),
        F.count("*").alias("num_rows"),
        F.min("created_at").alias("first_seen_2"),
    )
)
