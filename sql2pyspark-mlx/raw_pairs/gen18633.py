from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("has_fault").desc())
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select("id", "status_code", F.explode_outer("context").alias("key", "attr_value"))
)

parsed = (
    flattened
    .filter(F.col("key") == "version")
    .select(
        "id",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

kept_keys = (
    parsed
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") != 700)
        .select("id")
    )
)

reconciled = (
    parsed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("status_code").between(383, 433))
    .select(
        "id",
        "status_code",
        "status_code_high",
        "has_fault",
        "key_filled",
        "resolved_key",
        F.rank().over(w).alias("has_fault_rank"),
        F.rank().over(w2).alias("status_code_rank"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("level") == "ERROR")
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "status_code",
        "status_code_high",
        "key_filled",
        "has_fault_rank",
        "status_code_rank",
    )
    .orderBy(F.col("id").asc_nulls_first())
)
