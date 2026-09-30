from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("log_ts")).orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("status_code_high", "id", "k")
w3 = Window.partitionBy(F.to_date("log_ts"), "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_recent = Window.partitionBy("k").orderBy(F.col("flag_bit2").desc_nulls_first())
w5 = Window.partitionBy("k", "service").orderBy("status_code_high_avg_2h", "id", "k").rowsBetween(-3, -1)
w6 = Window.partitionBy("k").orderBy(F.col("first_known_flag_bit2").desc(), "id", "k")
w7 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc(), "id", "k")
w8 = Window.orderBy("first_known_flag_bit2", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w11 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

parsed = (
    lines
    .filter(F.col("service") != "search")
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

enriched = (
    parsed
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "status_code_high",
        F.first("flag_bit2", ignorenulls=True).over(w).alias("first_known_flag_bit2"),
        F.ntile(3).over(w2).alias("status_code_high_bucket"),
        F.avg("status_code_high").over(w3).alias("status_code_high_avg_2h"),
        F.percent_rank().over(w_recent).alias("flag_bit2_percent_rank"),
    )
)

with_windows = (
    enriched
    .withColumn("status_code_high_avg_2h_avg_3_prior", F.avg("status_code_high_avg_2h").over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("status_code_high_bucket_2", F.ntile(5).over(w7))
    .withColumn("cumulative_status_code_high_avg_2h", F.sum("status_code_high_avg_2h").over(w8))
    .withColumn("last_known_status_code_high", F.last("status_code_high", ignorenulls=True).over(w9))
)

gaps = (
    with_windows
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w10).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w10) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w11))

result = (
    islands
    .select(
        "id",
        "log_ts",
        "k",
        "status_code_high",
        "status_code_high_bucket",
        "status_code_high_avg_2h",
        "flag_bit2_percent_rank",
        "row_seq",
        "status_code_high_bucket_2",
    )
    .orderBy(
        F.col("status_code_high_bucket_2").desc(),
        F.col("flag_bit2_percent_rank").desc(),
    )
)
