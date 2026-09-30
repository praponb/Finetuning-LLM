from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes_2"),
    )
    .filter(F.col("span_minutes_2") > 0)
)
