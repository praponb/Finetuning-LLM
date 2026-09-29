from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("customer_id", "k").orderBy(F.col("days_since_created_at").desc())
w_hist = Window.partitionBy("customer_id").orderBy(F.col("is_device_count_1h").desc())
w5 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("k").isNotNull())
    .withColumn("is_device", F.when(F.col("k") == "device", 1).otherwise(0))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
)

valid_carts = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.col("k").eqNullSafe(F.lit("gift"))))
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "is_device",
        "days_since_created_at",
        F.coalesce("k", F.lit("Unknown")).alias("k_filled"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled_2"),
    )
)

scored = (
    valid_carts
    .filter(F.col("created_at") >= "2024-08-01")
    .select(
        "*",
        F.count("is_device").over(w).alias("is_device_count_1h"),
        F.avg("days_since_created_at").over(w2).alias("days_since_created_at_avg_15m"),
        F.percent_rank().over(w3).alias("days_since_created_at_percent_rank"),
    )
)

base = (
    scored
    .filter((F.col("created_at") > F.lit("2025-03-02").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "is_device",
        "days_since_created_at",
        "k_filled",
        "k_filled_2",
        "is_device_count_1h",
        "days_since_created_at_percent_rank",
        F.when(F.col("k").isin(["device", "gift"]), "Core").otherwise("Other").alias("k_group"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled_3"),
    )
)

valid_carts_2 = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.least("days_since_created_at_percent_rank", "days_since_created_at").alias("least_days_since_created_at_percent_rank_days_since_created_at"),
    )
)

enriched = (
    valid_carts_2
    .select(
        "customer_id",
        "k_filled_2",
        "days_since_created_at_percent_rank",
        "k_group",
        F.cume_dist().over(w_hist).alias("is_device_count_1h_cume_dist"),
        F.max("is_device").over(w5).alias("is_device_max_30m"),
    )
)

result = (
    enriched
    .withColumn("days_since_created_at_percent_rank_tier", F.when(F.col("days_since_created_at_percent_rank") >= 0.85, "LARGE").otherwise("SMALL"))
    .withColumn("is_device_max_30m_tier", F.when(F.col("is_device_max_30m") >= 82, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["days_since_created_at_percent_rank_tier", "is_device_max_30m_tier", "customer_id"], ["days_since_created_at_percent_rank_tier", "is_device_max_30m_tier"], ["days_since_created_at_percent_rank_tier"]],
        "days_since_created_at_percent_rank_tier",
        "is_device_max_30m_tier",
        "customer_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("days_since_created_at_percent_rank", [0.25, 0.5, 0.75]).alias("days_since_created_at_percent_rank_quartiles"),
        F.percentile_approx("is_device_max_30m", 0.9).alias("is_device_max_30m_p90"),
        F.grouping("days_since_created_at_percent_rank_tier").alias("grouping_id"),
    )
    .orderBy("is_device_max_30m_tier")
)
