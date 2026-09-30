from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("level", "host")
w3 = Window.partitionBy("service")
w4 = Window.partitionBy("host")
w5 = Window.partitionBy("service", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.orderBy("status_code_cume_dist")
w7 = Window.partitionBy(F.to_date("log_ts"), "key")
w8 = Window.partitionBy(F.date_trunc("MONTH", "log_ts"), "service")
w9 = Window.partitionBy(F.month("log_ts"), "level")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    flattened
    .withColumn("status_code_cume_dist", F.cume_dist().over(w))
    .withColumn("level_avg_status_code", F.avg("status_code").over(w2))
    .withColumn("service_sum_status_code", F.sum("status_code").over(w3))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4))
    .withColumn("status_code_max_30m", F.max("status_code").over(w5))
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("status_code_max_30m").over(w3).alias("service_max_status_code_max_30m"),
        F.cume_dist().over(w6).alias("status_code_cume_dist_cume_dist"),
        F.max("status_code").over(w7).alias("log_ts_to_date_max_status_code"),
        F.max("status_code").over(w8).alias("log_ts_period_max_status_code"),
        F.stddev_samp("level_avg_status_code").over(w9).alias("log_ts_month_stddev_samp_level_avg_status_code"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        "key",
        "v",
        "status_code_cume_dist",
        "service_sum_status_code",
        "service_max_status_code_max_30m",
        "log_ts_to_date_max_status_code",
        "log_ts_period_max_status_code",
        (F.col("log_ts_to_date_max_status_code") * F.col("status_code_share")).alias("log_ts_to_date_max_status_code_times_status_code_share"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "key",
        "v",
        "service_sum_status_code",
        "log_ts_period_max_status_code",
        F.when(F.col("key").rlike("^(trace|version)$"), "GROUP_A").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("key").desc(), "log_ts")
)
