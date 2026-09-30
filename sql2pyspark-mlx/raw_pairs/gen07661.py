from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("flag_bit2", "id", "key")
w_grp = Window.partitionBy("key").orderBy(F.col("status_code_high").desc())
w5 = Window.partitionBy("key").orderBy("new_group_flag", "id", "key").rowsBetween(-6, -1)
w_ordered = Window.partitionBy("key")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "log_ts",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

flagged = (
    parsed
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessionized
    .filter(F.col("flag_bit2") > 1)
    .select(
        "id",
        "key",
        "flag_bit2",
        "new_group_flag",
        F.ntile(5).over(w3).alias("flag_bit2_bucket"),
        F.cume_dist().over(w_grp).alias("status_code_high_cume_dist"),
        F.avg("flag_bit2").over(w5).alias("flag_bit2_avg_6_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "key",
        "new_group_flag",
        "flag_bit2_bucket",
        "flag_bit2_avg_6_prior",
        F.lead("flag_bit2_avg_6_prior").over(w3).alias("next_flag_bit2_avg_6_prior"),
        F.lag("flag_bit2").over(w3).alias("prev_flag_bit2"),
        F.row_number().over(w3).alias("seq_no"),
        F.stddev_samp("flag_bit2").over(w_ordered).alias("key_stddev_samp_flag_bit2"),
    )
)
