from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

parsed = (
    exploded
    .select(
        "log_ts",
        "level",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

kept_keys = (
    parsed
    .select("level")
    .intersect(
        df_app_logs
        .filter(F.hour("log_ts").between(12, 17))
        .select("level")
    )
)

verified = (
    parsed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("k").isNotNull())
    .select(
        "level",
        "k",
        "has_fault",
        "status_code_high",
        F.concat_ws("/", "level", "k").alias("level_k_key"),
    )
)

result = (
    valid
    .select("k", "has_fault", "status_code_high")
    .orderBy(F.col("has_fault").desc(), "k")
)
