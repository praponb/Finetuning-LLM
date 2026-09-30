from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.size("items").alias("num_items"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        F.avg("num_items").alias("avg_num_items"),
        F.min("created_at").alias("started_at"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.min("created_at").alias("first_seen"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("hour_window").asc_nulls_first())
)
