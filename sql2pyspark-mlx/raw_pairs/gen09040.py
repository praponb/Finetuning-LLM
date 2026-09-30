from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code_plus_status_code_high").desc_nulls_first(), "id", "k")
w2 = Window.partitionBy("k")
w_recent = Window.orderBy("status_code_plus_status_code_high", "id", "k")
w4 = Window.partitionBy("k").orderBy("status_code_plus_status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("status_code_plus_status_code_high", "id", "k")
w6 = Window.partitionBy("k").orderBy("next_status_code_plus_status_code_high_2", "id", "k").rowsBetween(-5, -1)
w7 = Window.partitionBy("k").orderBy(F.col("k_stddev_samp_next_status_code_plus_status_code_high_2").desc(), "id", "k")
w_part = Window.partitionBy("k").orderBy("next_status_code_plus_status_code_high_2", "id", "k").rowsBetween(-6, -1)

lines = (
    df_app_logs
    .select("id", "status_code", F.explode("context").alias("k", "attr_value"))
)

unpacked = (
    lines
    .select(
        "id",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    unpacked
    .filter(~F.col("k").eqNullSafe(F.lit("version")))
    .select(
        "id",
        "k",
        (F.col("status_code") + F.col("status_code_high")).alias("status_code_plus_status_code_high"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "k", "status_code_plus_status_code_high")
)

metrics = (
    latest
    .filter(F.col("k").isin(["trace", "region", "version"]))
    .select(
        "id",
        "k",
        "status_code_plus_status_code_high",
        F.sum("status_code_plus_status_code_high").over(w2).alias("k_sum_status_code_plus_status_code_high"),
        F.lag("status_code_plus_status_code_high", 1).over(w_recent).alias("prev_status_code_plus_status_code_high"),
        F.lead("status_code_plus_status_code_high", 2).over(w_recent).alias("next_status_code_plus_status_code_high"),
        F.last("status_code_plus_status_code_high", ignorenulls=True).over(w4).alias("last_known_status_code_plus_status_code_high"),
        F.lead("status_code_plus_status_code_high", 1, 0).over(w5).alias("next_status_code_plus_status_code_high_2"),
    )
)

metrics_app_logs = (
    metrics
    .select(
        "id",
        "k",
        "next_status_code_plus_status_code_high",
        "last_known_status_code_plus_status_code_high",
        "next_status_code_plus_status_code_high_2",
        F.max("last_known_status_code_plus_status_code_high").over(w2).alias("k_max_last_known_status_code_plus_status_code_high"),
        F.avg("k_sum_status_code_plus_status_code_high").over(w6).alias("k_sum_status_code_plus_status_code_high_avg_5_prior"),
        F.stddev_samp("next_status_code_plus_status_code_high_2").over(w2).alias("k_stddev_samp_next_status_code_plus_status_code_high_2"),
    )
)

result = (
    metrics_app_logs
    .select(
        "id",
        "k",
        "next_status_code_plus_status_code_high",
        "k_max_last_known_status_code_plus_status_code_high",
        "k_sum_status_code_plus_status_code_high_avg_5_prior",
        "k_stddev_samp_next_status_code_plus_status_code_high_2",
        F.row_number().over(w7).alias("event_seq"),
        F.avg("next_status_code_plus_status_code_high").over(w_part).alias("next_status_code_plus_status_code_high_avg_6_prior"),
    )
)
