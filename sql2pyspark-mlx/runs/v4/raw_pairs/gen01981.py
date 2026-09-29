from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.partitionBy("level")
w3 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

filtered = (
    exploded
    .filter(F.col("key").isNotNull())
    .withColumn("log_ts_month", F.month("log_ts"))
)

enriched = (
    filtered
    .withColumn("latency_ms_avg_30m", F.avg("latency_ms").over(w_grp))
    .withColumn("level_sum_latency_ms", F.sum("latency_ms").over(w_recent))
    .withColumn("service_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w3))
)

by_type = (
    enriched
    .groupBy("key")
    .pivot("host", ["h2", "h4"])
    .agg(F.sum("log_ts_month").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = (
    by_type
    .select(
        "key",
        "h2_total",
        "h2_n",
        "h4_total",
        "h4_n",
        F.when(F.col("key").rlike("^(trace|version)$"), "CORE").otherwise("GROUP_A").alias("key_zone"),
    )
    .orderBy(F.col("key").desc())
)
