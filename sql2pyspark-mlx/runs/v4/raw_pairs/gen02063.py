from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01 14:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select("*", F.month("created_at").alias("created_at_month"))
)

filtered = (
    valid
    .filter(F.col("customer_id") == 17)
    .withColumn("key_lower", F.lower("key"))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
)

base = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("key").isNotNull())
    .select(
        "id",
        "created_at",
        "key",
        "attr_value",
        "created_at_month",
        "key_lower",
        "days_since_created_at",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at_2"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

valid_carts = (
    base
    .filter((F.col("created_at") > F.lit("2025-03-01 12:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select(
        "*",
        F.round((F.col("days_since_created_at_2") * 1.07), 2).alias("days_since_created_at_2_scaled"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

result = (
    valid_carts
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .rollup("hour_window")
    .agg(
        F.round(F.avg("days_since_created_at"), 2).alias("avg_days_since_created_at"),
        F.countDistinct("key").alias("distinct_keys"),
        F.sum(F.when(F.col("key") == "weight_kg", F.col("created_at_month"))).alias("weight_kg_total_created_at_month"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("avg_days_since_created_at") < 0)
)
