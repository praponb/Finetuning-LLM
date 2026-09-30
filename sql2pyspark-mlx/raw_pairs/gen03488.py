from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(-2, -1)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "attr_key",
        F.lead("status_code", 1, 0).over(w_part).alias("next_status_code"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code"),
        F.avg("status_code").over(w3).alias("status_code_avg_2_prior"),
    )
)

remaining_keys = (
    metrics
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") >= 360)
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter((F.col("attr_key").eqNullSafe(F.lit("region"))) & (F.col("status_code_avg_2_prior") <= 276))
    .withColumn("corr_latency_ms_status_code_filled", F.coalesce("corr_latency_ms_status_code", F.lit(0)))
    .withColumn("is_region", F.when(F.col("attr_key") == "region", 1).otherwise(0))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
)

result = (
    base
    .select(
        "service",
        "corr_latency_ms_status_code",
        "corr_latency_ms_status_code_filled",
    )
    .unpivot(
        ids=["service"],
        values=["corr_latency_ms_status_code", "corr_latency_ms_status_code_filled"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
