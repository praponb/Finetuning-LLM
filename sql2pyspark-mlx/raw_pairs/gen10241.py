from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("created_at") < "2023-09-01")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_seen"),
        F.max("created_at").alias("last_created_at"),
        F.min("created_at").alias("first_seen_2"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.count("*").alias("row_count"),
    )
    .orderBy("cnt_2")
)
