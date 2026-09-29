from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "level")
w2 = Window.partitionBy("key")
w3 = Window.orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("level")
w_running = Window.orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

scored = (
    exploded
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w))
    .withColumn("key_sum_latency_ms", F.sum("latency_ms").over(w2))
)

matched_keys = (
    scored
    .select("level", "id")
    .intersectAll(
        df_app_logs
        .filter(F.col("status_code") >= 409)
        .select("level", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("latency_ms_rank", F.rank().over(w3))
    .withColumn("key_sum_latency_ms_share", F.round((F.col("key_sum_latency_ms") / F.sum("key_sum_latency_ms").over(w4)), 4))
    .withColumn("status_code_rank", F.rank().over(w_running))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w4))
)

result = (
    metrics
    .select(
        "level",
        F.col("corr_latency_ms_status_code").alias("CORR_LATENCY_MS_STATUS_CODE"),
        F.col("key_sum_latency_ms_share").alias("KEY_SUM_LATENCY_MS_SHARE"),
        F.col("corr_status_code_latency_ms").alias("CORR_STATUS_CODE_LATENCY_MS"),
    )
    .unpivot(
        ["level"],
        ["CORR_LATENCY_MS_STATUS_CODE", "KEY_SUM_LATENCY_MS_SHARE", "CORR_STATUS_CODE_LATENCY_MS"],
        "metric_name",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
