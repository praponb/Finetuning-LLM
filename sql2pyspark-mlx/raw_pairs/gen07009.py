from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "host", F.explode_outer("context").alias("attr_key", "value"))
)

valid = (
    lines
    .filter(~F.col("attr_key").eqNullSafe(F.lit("trace")))
    .withColumn("attr_key_host", F.concat("attr_key", F.lit(" "), "host"))
)

kept_keys = (
    valid
    .select("host", "id")
    .subtract(
        df_app_logs
        .filter(F.col("host") == "h4")
        .select("host", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["ERROR", "WARN"]))
        .select("id")
    )
)

kept_rows_app_logs = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = kept_rows_app_logs.select("value")
