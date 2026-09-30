from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "log_ts", "status_code", F.explode_outer("context").alias("k", "v"))
)

unpacked = (
    exploded
    .select(
        "id",
        "status_code",
        "k",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

valid = (
    unpacked
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "status_code",
        "k",
        "v",
        "status_code_high",
        "is_flagged",
        F.when(F.col("k") == "trace", 1).otherwise(0).alias("is_trace"),
        F.floor("is_flagged").alias("is_flagged_whole"),
    )
)

base = (
    valid
    .filter(F.col("k").isNotNull() & F.col("is_flagged_whole").between(44, 59))
    .withColumn("is_flagged_whole_times_is_flagged", (F.col("is_flagged_whole") * F.col("is_flagged")))
    .withColumn("status_code_high_whole", F.floor("status_code_high"))
)

result = (
    base
    .groupBy(
        F.when(F.col("status_code") >= 421, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.sum(F.when(F.col("k") == "version", F.col("is_flagged"))).alias("version_total_is_flagged"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.count_if(F.col("k") == "version").alias("version_events"),
    )
    .orderBy(F.col("version_total_is_flagged").desc())
)
