from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("is_trace").desc_nulls_first())

flattened = (
    df_app_logs
    .select("id", "service", F.explode("context").alias("attr_key", "attr_value"))
)

base = (
    flattened
    .filter(~F.col("attr_key").isin(["version", "region"]))
    .withColumn("service_lower", F.lower("service"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

filtered = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "attr_key",
        "attr_value",
        "service_lower",
        "resolved_attr_key",
        F.when(F.col("attr_key") == "trace", 1).otherwise(0).alias("is_trace"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

base_app_logs = (
    latest
    .filter(F.col("is_trace").between(54, 80))
    .select(
        "id",
        "attr_key",
        "attr_value",
        "service_lower",
        "resolved_attr_key",
        (F.col("dedup_rank") / F.nullif("is_trace", F.lit(0))).alias("dedup_rank_per_is_trace"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("attr_key_filled"),
    )
)

filtered_app_logs = (
    base_app_logs
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "attr_key",
        "attr_value",
        "resolved_attr_key",
        "dedup_rank_per_is_trace",
        "attr_key_filled",
        F.ceil("dedup_rank_per_is_trace").alias("dedup_rank_per_is_trace_whole"),
    )
)

result = (
    filtered_app_logs
    .rollup(
        F.when(F.col("dedup_rank_per_is_trace") >= 60, "HIGH").otherwise("LOW").alias("dedup_rank_per_is_trace_tier"),
        F.when(F.col("dedup_rank_per_is_trace_whole") >= 83, "LARGE").otherwise("SMALL").alias("dedup_rank_per_is_trace_whole_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.percentile_approx("dedup_rank_per_is_trace", [0.25, 0.5, 0.75]).alias("dedup_rank_per_is_trace_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("dedup_rank_per_is_trace_whole")).otherwise(0)).alias("region_dedup_rank_per_is_trace_whole"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
    )
    .orderBy(F.col("dedup_rank_per_is_trace_whole_tier").desc_nulls_last())
)
