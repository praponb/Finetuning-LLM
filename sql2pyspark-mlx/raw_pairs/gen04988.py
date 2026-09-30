from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

base = (
    lines
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 15:00:00"))) & F.col("attr_key").isNotNull())
    .withColumn("is_vip", F.array_contains("coupon_codes", "VIP"))
    .withColumn("attr_key_title", F.initcap(F.trim("attr_key")))
)

valid = (
    base
    .filter(F.col("created_at") < "2025-01-01")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attr_key",
        "value",
        "is_vip",
        "attr_key_title",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.when(F.col("is_vip"), "Yes").otherwise("No").alias("is_vip_label"),
    )
)

base_carts = (
    valid
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("attr_key").isNotNull())
    .select(
        "*",
        F.col("is_vip").cast("int").alias("is_vip_flag"),
        F.datediff(F.current_date(), "created_at_day").alias("days_since_created_at_day"),
    )
)

stats = (
    base_carts
    .groupBy(
        F.when(F.col("days_since_created_at_day") >= 83, "LARGE").otherwise("SMALL").alias("days_since_created_at_day_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("created_at").alias("first_seen"),
        F.sum(F.when(F.col("attr_key") == "device", F.col("is_vip_flag")).otherwise(0)).alias("device_is_vip_flag"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.min("days_since_created_at_day").alias("min_days_since_created_at_day"),
    )
)

result = (
    stats
    .select(
        "days_since_created_at_day_tier",
        "distinct_ids",
        "device_is_vip_flag",
        "device_rows",
    )
)
