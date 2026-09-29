from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.year("log_ts"), "level").orderBy(F.col("status_code_high").asc_nulls_last())

exploded = (
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

flattened_attrs = (
    exploded
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

matched_keys = (
    flattened_attrs
    .select("level", "id")
    .subtract(
        df_app_logs
        .filter(F.col("log_ts") < "2025-04-01")
        .select("level", "id")
    )
)

verified = (
    flattened_attrs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = verified.withColumn("status_code_high_rank", F.rank().over(w_running))

base = (
    enriched
    .filter((F.col("log_ts") > F.lit("2025-06-01 08:00:00").cast("timestamp")) & F.col("level").isin(["ERROR", "WARN"]))
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "status_code_high",
        "is_flagged",
        F.concat_ws("-", "attr_key", "level").alias("attr_key_level_key"),
        F.greatest("status_code_high", "is_flagged").alias("greatest_status_code_high_is_flagged"),
    )
)

result = (
    base
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .cube("activity_day")
    .agg(
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_rows"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.min("log_ts").alias("first_seen"),
        F.grouping_id().alias("grouping_id"),
    )
)
