from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("rn", "id", "attr_key")
w3 = Window.partitionBy("service", "attr_key")
w4 = Window.orderBy("status_code", "id", "attr_key")
w5 = Window.orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(-3, -1)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

deduped = (
    flattened
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .filter(F.col("attr_key") == "trace")
    .withColumn("next_latency_ms", F.lead("latency_ms", 1).over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
    .withColumn("status_code_bucket", F.ntile(4).over(w4))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w5))
    .withColumn("status_code_avg_3_prior", F.avg("status_code").over(w6))
)

cleaned = (
    metrics
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("region"))))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        "value",
        "rn",
        "next_latency_ms",
        "latency_ms_share",
        "cumulative_status_code",
        "status_code_avg_3_prior",
        F.round((F.col("status_code_bucket") * 100), 2).alias("status_code_bucket_scaled"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "attr_key",
        "latency_ms_share",
        F.col("status_code_bucket_scaled").alias("STATUS_CODE_BUCKET_SCALED"),
    )
    .unpivot(
        ids=["id", "attr_key"],
        values=["latency_ms_share", "STATUS_CODE_BUCKET_SCALED"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
