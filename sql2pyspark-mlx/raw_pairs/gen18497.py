from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service")
w2 = Window.partitionBy("service").orderBy(F.col("has_fault").asc_nulls_first())
w3 = Window.partitionBy("service", "host").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("host").orderBy(F.col("has_fault").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

kept_keys = (
    flattened_attrs
    .select("service")
    .intersectAll(
        df_app_logs
        .filter(F.col("latency_ms") == 677)
        .select("service")
    )
)

verified = (
    flattened_attrs
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("status_code_high") > 1)
    .select(
        "*",
        F.stddev_samp("status_code").over(w).alias("host_stddev_samp_status_code"),
        F.percent_rank().over(w2).alias("has_fault_percent_rank"),
        F.rank().over(w3).alias("status_code_rank"),
        F.cume_dist().over(w4).alias("has_fault_cume_dist"),
    )
)

valid = (
    with_windows
    .filter(F.col("latency_ms").between(1750, 2087))
    .select("*", F.floor("latency_ms").alias("latency_ms_whole"))
)

result = (
    valid
    .select(
        "service",
        "has_fault_percent_rank",
        F.col("has_fault_cume_dist").alias("HAS_FAULT_CUME_DIST_VAL"),
    )
    .unpivot(
        ids=["service"],
        values=["has_fault_percent_rank", "HAS_FAULT_CUME_DIST_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
