from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("device")
    .subtract(
        df_sessions
        .filter(F.col("country").isNull())
        .select("device")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("device", "started_at")
    .orderBy(F.col("started_at").desc())
)
