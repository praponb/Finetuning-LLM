from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "host", F.explode("context").alias("attr_key", "attr_value"))
)

cleaned = (
    lines
    .filter(~F.col("attr_key").isin(["version", "region"]))
    .withColumn("host_upper", F.upper("host"))
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .withColumn("host_prefix", F.substring("host", 1, 4))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("unassigned")))
)

remaining_keys = (
    cleaned_app_logs
    .select("host", "id")
    .exceptAll(
        df_app_logs
        .filter(F.hour("log_ts").between(6, 11))
        .select("host", "id")
    )
)

reconciled = (
    cleaned_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("attr_key", "host_upper", "attr_key_filled")
    .orderBy(F.col("attr_key").asc_nulls_first())
)
