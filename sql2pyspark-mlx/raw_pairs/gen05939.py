from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("flag_bit2").asc_nulls_last())
w2 = Window.partitionBy("level")
w3 = Window.orderBy(F.col("flag_bit2_cume_dist").desc_nulls_first())
w4 = Window.orderBy(F.col("flag_bit2").desc_nulls_first())
w5 = Window.orderBy("level_stddev_samp_status_code_high_minus_flag_bit2")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

parsed = (
    exploded
    .select(
        "service",
        "level",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

cleaned = (
    parsed
    .filter(F.col("status_code") < 292)
    .withColumn("least_status_code_high_status_code", F.least("status_code_high", "status_code"))
)

base = (
    cleaned
    .filter(F.col("status_code_high") == 2)
    .withColumn("service_title", F.initcap(F.trim("service")))
)

metrics = (
    base
    .filter(F.col("least_status_code_high_status_code") < 46)
    .select(
        "flag_bit2",
        "service_title",
        F.cume_dist().over(w).alias("flag_bit2_cume_dist"),
        F.stddev_samp("status_code_high").over(w2).alias("level_stddev_samp_status_code_high"),
    )
)

valid = (
    metrics
    .filter(F.col("flag_bit2") >= 1)
    .withColumn("level_stddev_samp_status_code_high_minus_flag_bit2", (F.col("level_stddev_samp_status_code_high") - F.col("flag_bit2")))
)

metrics_app_logs = (
    valid
    .select(
        "flag_bit2",
        "level_stddev_samp_status_code_high",
        F.cume_dist().over(w3).alias("flag_bit2_cume_dist_cume_dist"),
        F.dense_rank().over(w4).alias("flag_bit2_dense_rank"),
        F.dense_rank().over(w5).alias("level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank"),
        F.percent_rank().over(w5).alias("level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank"),
    )
)

daily = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("flag_bit2_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("flag_bit2_dense_rank_tier"),
        F.when(F.col("level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank") >= 0.87, "PREMIUM").otherwise("STANDARD").alias("level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("flag_bit2", 0.5).alias("flag_bit2_p50"),
        F.percentile_approx("level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank", 0.9).alias("level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank_p90"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 1)
)

result = (
    daily
    .select(
        "flag_bit2_dense_rank_tier",
        "level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank_tier",
        "flag_bit2_p50",
        "level_stddev_samp_status_code_high_minus_flag_bit2_dense_rank_p90",
        "num_rows",
        "row_count",
    )
    .orderBy("level_stddev_samp_status_code_high_minus_flag_bit2_percent_rank_tier")
)
