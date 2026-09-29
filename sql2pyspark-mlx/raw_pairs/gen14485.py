from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("log_ts"), "host")

flattened = (
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
    flattened
    .filter(F.col("host").isin(["h1", "h2"]))
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    flattened_attrs
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (F.col("flag_bit1") >= 1))
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "flag_bit1",
        "status_code_high",
        (F.col("status_code_high") + F.col("flag_bit1")).alias("status_code_high_plus_flag_bit1"),
    )
)

metrics = (
    cleaned
    .filter(F.col("status_code_high") <= 1)
    .withColumn("log_ts_to_date_stddev_samp_status_code_high_plus_flag_bit1", F.stddev_samp("status_code_high_plus_flag_bit1").over(w))
)

result = (
    metrics
    .withColumn("status_code_high_plus_flag_bit1_tier", F.when(F.col("status_code_high_plus_flag_bit1") >= 72, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["host", "status_code_high_plus_flag_bit1_tier"], ["host"]],
        "host",
        "status_code_high_plus_flag_bit1_tier",
    )
    .agg(
        F.percentile_approx("flag_bit1", 0.9).alias("flag_bit1_p90"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt_2"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
