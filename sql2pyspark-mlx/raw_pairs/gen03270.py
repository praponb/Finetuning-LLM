from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("has_fault_minus_status_code_high").asc_nulls_first())
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("k").orderBy(F.col("has_fault_minus_status_code_high").asc_nulls_first())
w4 = Window.orderBy(F.col("has_fault_minus_status_code_high").desc())
w5 = Window.partitionBy("k").orderBy(F.col("status_code_high").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "host",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("host").isin(["h2", "h3"]))
    .withColumn("has_fault_minus_status_code_high", (F.col("has_fault") - F.col("status_code_high")))
    .withColumn("k_filled", F.coalesce("k", F.lit("DIRECT")))
)

enriched = (
    valid
    .select(
        "*",
        F.cume_dist().over(w).alias("has_fault_minus_status_code_high_cume_dist"),
        F.max("status_code_high").over(w2).alias("host_max_status_code_high"),
        F.rank().over(w3).alias("has_fault_minus_status_code_high_rank"),
        F.rank().over(w4).alias("has_fault_minus_status_code_high_rank_2"),
        F.dense_rank().over(w5).alias("status_code_high_dense_rank"),
    )
)

base = (
    enriched
    .filter(F.col("k") == "version")
    .select(
        "id",
        "host",
        "k",
        "has_fault",
        "status_code_high",
        "has_fault_minus_status_code_high_cume_dist",
        "has_fault_minus_status_code_high_rank",
        "has_fault_minus_status_code_high_rank_2",
        "status_code_high_dense_rank",
        F.trim("k").alias("clean_k"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

base_app_logs = (
    base
    .filter(F.col("has_fault_minus_status_code_high_rank_2") >= 4)
    .withColumn("status_code_high_dense_rank_per_status_code_high", (F.col("status_code_high_dense_rank") / F.nullif("status_code_high", F.lit(0))))
    .withColumn("k_clean", F.coalesce("k", F.lit("UNKNOWN")))
)

result = (
    base_app_logs
    .groupBy("host")
    .agg(
        F.percentile_approx("status_code_high_dense_rank_per_status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_dense_rank_per_status_code_high_quartiles"),
        F.percentile_approx("has_fault", 0.95).alias("has_fault_p95"),
        F.round(F.avg("has_fault_minus_status_code_high_cume_dist"), 2).alias("avg_has_fault_minus_status_code_high_cume_dist"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("host") == "h3", F.col("status_code_high"))).alias("h3_total_status_code_high"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
