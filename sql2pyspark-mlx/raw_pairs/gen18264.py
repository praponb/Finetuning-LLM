from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("is_error").desc_nulls_first(), "id", "k")
w2 = Window.partitionBy("k").orderBy(F.col("is_error").desc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("is_error").desc(), "id", "k")

lines = (
    df_app_logs
    .select("id", "level", "latency_ms", F.explode("context").alias("k", "attr_value"))
)

filtered = (
    lines
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        F.concat("k", F.lit(" "), "level").alias("k_level"),
        F.coalesce("k", F.lit("N/A")).alias("resolved_k"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("latency_ms").between(1011, 2254))
    .select(
        "id",
        "k",
        "resolved_k",
        F.when(F.col("level") == "ERROR", 1).otherwise(0).alias("is_error"),
    )
)

deduped = (
    filtered_app_logs
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

latest_app_logs = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

grouped = (
    latest_app_logs
    .withColumn("is_error_tier", F.when(F.col("is_error") >= 73, "LARGE").otherwise("SMALL"))
    .groupingSets([["k", "is_error_tier"], ["k"], []], "k", "is_error_tier")
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count_if(F.col("k") == "region").alias("region_events"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
    )
    .filter(F.col("region_events") >= 1)
)

result = grouped.select("k", "is_error_tier", "num_ks")
