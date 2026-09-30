from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("attr_key", "service").orderBy("status_code_high", "id", "attr_key").rowsBetween(-5, -1)
w3 = Window.partitionBy("service").orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key", "service").orderBy(F.col("status_code_high").desc(), "id", "attr_key")
w5 = Window.partitionBy("attr_key", "service").orderBy("status_code_high", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms_avg_5_prior").desc_nulls_last(), "id", "attr_key")
w7 = Window.partitionBy("attr_key")
w8 = Window.partitionBy("attr_key").orderBy("last_known_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last_known_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    parsed
    .select(
        "id",
        "service",
        "attr_key",
        "status_code_high",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_5_prior"),
        F.last("latency_ms", ignorenulls=True).over(w3).alias("last_known_latency_ms"),
        F.row_number().over(w4).alias("row_seq"),
        F.lag("status_code", 1).over(w5).alias("prev_status_code"),
    )
)

valid = (
    enriched
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "attr_key",
        "status_code_high",
        "latency_ms_share",
        "latency_ms_avg_5_prior",
        "last_known_latency_ms",
        "row_seq",
        (F.col("prev_status_code") * F.col("latency_ms_share")).alias("prev_status_code_times_latency_ms_share"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "attr_key",
        "latency_ms_share",
        "latency_ms_avg_5_prior",
        "last_known_latency_ms",
        "row_seq",
    )
)

cleaned = (
    ranked
    .filter(F.col("row_seq") >= 4)
    .select(
        "id",
        "attr_key",
        "latency_ms_avg_5_prior",
        "last_known_latency_ms",
        "row_seq",
        F.upper("attr_key").alias("attr_key_upper"),
    )
)

enriched_app_logs = (
    cleaned
    .select(
        "*",
        F.count("row_seq").over(w7).alias("attr_key_count_row_seq"),
        F.last("last_known_latency_ms", ignorenulls=True).over(w8).alias("last_known_last_known_latency_ms"),
        F.last("attr_key", ignorenulls=True).over(w9).alias("last_known_attr_key"),
    )
)

result = (
    enriched_app_logs
    .select(
        "id",
        "attr_key",
        "latency_ms_avg_5_prior",
        "row_seq",
        "attr_key_upper",
        "attr_key_count_row_seq",
        "last_known_last_known_latency_ms",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "CORE").when(F.col("attr_key").rlike("^(trace)$"), "GROUP_A").otherwise("PRIORITY").alias("attr_key_zone"),
    )
)
