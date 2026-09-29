from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level", "attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy(F.col("latency_ms_num").desc_nulls_last(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

scored = (
    flattened
    .withColumn("running_count_latency_ms", F.count("latency_ms").over(w_grp))
    .withColumn("running_avg_latency_ms", F.avg("latency_ms").over(w_grp))
    .withColumn("running_max_latency_ms", F.max("latency_ms").over(w_grp))
    .withColumn("running_min_latency_ms", F.min("latency_ms").over(w_grp))
)

valid = (
    scored
    .filter(~F.col("attr_key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "attr_key",
        "attr_value",
        "running_max_latency_ms",
        "running_min_latency_ms",
        F.when(F.col("running_min_latency_ms") >= 1850, "High").when(F.col("running_min_latency_ms") >= 960, "Medium").otherwise("Low").alias("running_min_latency_ms_tier"),
        F.col("latency_ms").cast("double").alias("latency_ms_num"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    latest
    .select("host")
    .intersectAll(
        df_app_logs
        .filter(F.col("level").isin(["WARN", "DEBUG", "ERROR"]))
        .select("host")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "level",
        F.when(F.col("running_max_latency_ms") >= 2230, "PREMIUM").otherwise("STANDARD").alias("running_max_latency_ms_tier"),
    )
    .agg(
        F.sum(F.when(F.col("attr_key") == "trace", F.col("running_max_latency_ms")).otherwise(0)).alias("trace_running_max_latency_ms"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
        F.countDistinct("host").alias("distinct_hosts"),
    )
    .orderBy(F.col("trace_running_max_latency_ms").asc_nulls_first())
)
