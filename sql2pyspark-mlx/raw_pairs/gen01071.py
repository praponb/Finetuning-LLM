from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "k")
w2 = Window.partitionBy("k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

base = (
    flattened
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 11:00:00"))) & F.col("k").isNotNull())
    .withColumn("k_prefix", F.substring("k", 1, 2))
)

scored = (
    base
    .filter(F.col("service").isin(["auth", "checkout"]))
    .select(
        "id",
        "host",
        "value",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.min("latency_ms").over(w2).alias("k_min_latency_ms"),
    )
)

remaining_keys = (
    scored
    .select("id")
    .exceptAll(
        df_app_logs
        .filter(~F.col("service").isin(["search", "checkout", "auth"]))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("status_code_share") >= 0.68, "PREMIUM").otherwise("STANDARD").alias("status_code_share_tier"),
    )
    .agg(
        F.sum(F.when(F.col("host") == "h4", F.col("status_code_share")).otherwise(0)).alias("h4_status_code_share"),
        F.round(F.avg("k_min_latency_ms"), 2).alias("avg_k_min_latency_ms"),
        F.percentile_approx("status_code_share", [0.25, 0.5, 0.75]).alias("status_code_share_quartiles"),
    )
    .orderBy(F.col("avg_k_min_latency_ms").desc())
)
