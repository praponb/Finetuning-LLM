from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        "product_id",
    )
    .agg(
        F.avg("qty").alias("avg_qty"),
        F.min("created_at").alias("first_seen"),
        F.max("product_id").alias("max_product_id"),
    )
)
