from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.orderBy(F.col("has_fault").desc_nulls_first())
w3 = Window.partitionBy("attr_key").orderBy(F.col("has_fault").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "latency_ms",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    parsed
    .select(
        "id",
        "has_fault",
        "status_code_high",
        F.sum("has_fault").over(w).alias("attr_key_sum_has_fault"),
        F.cume_dist().over(w2).alias("has_fault_cume_dist"),
        F.rank().over(w3).alias("has_fault_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("has_fault") != 0)
    .select(
        "*",
        F.round((F.col("has_fault_rank") * 1.15), 2).alias("has_fault_rank_adjusted"),
        F.round((F.col("attr_key_sum_has_fault") * 0.9), 2).alias("adjusted_attr_key_sum_has_fault"),
    )
)

cleaned = (
    filtered
    .filter(F.col("has_fault_rank") != 3)
    .withColumn("adjusted_attr_key_sum_has_fault_minus_status_code_high", (F.col("adjusted_attr_key_sum_has_fault") - F.col("status_code_high")))
    .withColumn("least_has_fault_rank_has_fault", F.least("has_fault_rank", "has_fault"))
)

filtered_app_logs = (
    cleaned
    .filter(F.col("adjusted_attr_key_sum_has_fault_minus_status_code_high") >= 31)
    .select(
        "id",
        "has_fault",
        "status_code_high",
        "attr_key_sum_has_fault",
        "has_fault_cume_dist",
        "adjusted_attr_key_sum_has_fault",
        "adjusted_attr_key_sum_has_fault_minus_status_code_high",
        "least_has_fault_rank_has_fault",
        F.round((F.col("least_has_fault_rank_has_fault") / F.col("has_fault_rank")), 2).alias("least_has_fault_rank_has_fault_per_has_fault_rank"),
        (F.col("attr_key_sum_has_fault") / F.nullif("has_fault_cume_dist", F.lit(0))).alias("attr_key_sum_has_fault_per_has_fault_cume_dist"),
    )
)

result = (
    filtered_app_logs
    .withColumn("adjusted_attr_key_sum_has_fault_tier", F.when(F.col("adjusted_attr_key_sum_has_fault") >= 85, "HIGH").otherwise("LOW"))
    .withColumn("has_fault_cume_dist_tier", F.when(F.col("has_fault_cume_dist") >= 0.67, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["adjusted_attr_key_sum_has_fault_tier", "has_fault_cume_dist_tier"], ["adjusted_attr_key_sum_has_fault_tier"], []],
        "adjusted_attr_key_sum_has_fault_tier",
        "has_fault_cume_dist_tier",
    )
    .agg(
        F.sum("attr_key_sum_has_fault").alias("total_attr_key_sum_has_fault"),
        F.count("*").alias("cnt"),
        F.round(F.avg("attr_key_sum_has_fault_per_has_fault_cume_dist"), 2).alias("avg_attr_key_sum_has_fault_per_has_fault_cume_dist"),
        F.count("*").alias("event_count"),
        F.round(F.avg("has_fault_cume_dist"), 2).alias("avg_has_fault_cume_dist"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_has_fault_cume_dist").asc_nulls_last())
)
