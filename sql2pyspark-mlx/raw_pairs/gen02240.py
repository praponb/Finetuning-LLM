from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("host").orderBy(F.col("status_code_rounded").desc())
w2 = Window.partitionBy("level")
w_grp = Window.orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter((F.col("host") != "h3") & F.col("key").isNotNull())
    .select(
        "host",
        "level",
        "status_code",
        F.concat("host", F.lit(" "), "level").alias("host_level"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

valid = (
    cleaned
    .filter(F.col("host") == "h4")
    .withColumn("is_debug", F.when(F.col("level") == "DEBUG", 1).otherwise(0))
    .withColumn("status_code_rounded", F.round("status_code", 1))
)

kept_keys = (
    valid
    .select("host", "level")
    .subtract(
        df_app_logs
        .filter(F.col("service").isin(["checkout", "auth", "billing"]))
        .select("host", "level")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("status_code_rounded_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("level_count_is_debug", F.count("is_debug").over(w2))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w_grp))
)

result = (
    with_windows
    .select(
        "host",
        "host_level",
        "resolved_key",
        "is_debug",
        "status_code_rounded",
        "level_count_is_debug",
        "status_code_dense_rank",
    )
    .orderBy(F.col("status_code_dense_rank").desc(), "status_code_rounded")
)
