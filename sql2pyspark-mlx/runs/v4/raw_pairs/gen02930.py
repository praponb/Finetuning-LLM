from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

result = (
    lines
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .cube("hour_window", "key")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
        F.grouping("hour_window").alias("gid"),
    )
    .orderBy(F.col("row_count").asc_nulls_first())
)
