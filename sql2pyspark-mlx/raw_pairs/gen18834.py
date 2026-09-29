from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .groupingSets(
        [["customer_id", "hour_window"], ["customer_id"], []],
        "customer_id",
        "hour_window",
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.size(F.collect_set("key")).alias("num_keys"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min_2"),
        F.count_if(F.col("key") == "utm_source").alias("utm_source_events"),
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_count"),
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("weight_kg_count").asc_nulls_last())
)
