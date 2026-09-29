from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "attr_key").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("attr_key", "level").orderBy("status_code", "id", "attr_key").rowsBetween(-6, -1)
w3 = Window.partitionBy("attr_key", "level").orderBy(F.col("status_code").desc(), "id", "attr_key")
w4 = Window.partitionBy(F.year("log_ts"), "attr_key")
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy(F.col("event_seq").asc_nulls_first(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

cleaned = (
    flattened
    .filter((F.col("log_ts") < "2024-01-01") & F.col("attr_key").isNotNull())
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        "value",
        F.substring("host", 1, 3).alias("host_prefix"),
    )
)

metrics = (
    cleaned
    .filter(F.col("status_code") > 283)
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        F.row_number().over(w).alias("event_seq"),
        F.avg("status_code").over(w2).alias("status_code_avg_6_prior"),
        F.row_number().over(w3).alias("row_seq"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped_app_logs = (
    deduped
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

grouped = (
    deduped_app_logs
    .groupBy(
        F.when(F.col("status_code_avg_6_prior") >= 422, "LARGE").otherwise("SMALL").alias("status_code_avg_6_prior_tier"),
        "attr_key",
        F.when(F.col("status_code_share") >= 0.89, "PREMIUM").otherwise("STANDARD").alias("status_code_share_tier"),
    )
    .agg(
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.min("log_ts").alias("first_seen"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("level") == "WARN", 1)), F.lit(0)), 2).alias("status_code_per_warn"),
        F.countDistinct("level").alias("distinct_levels"),
    )
)

result = (
    grouped
    .select(
        "status_code_avg_6_prior_tier",
        "attr_key",
        "status_code_share_tier",
        "warn_count",
        "first_seen",
        "status_code_per_warn",
        "distinct_levels",
    )
    .orderBy("status_code_avg_6_prior_tier")
)
