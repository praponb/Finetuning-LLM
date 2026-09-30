from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.year("log_ts"), "k").orderBy("log_ts", "id", "k").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("k").orderBy("new_group_flag", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service").orderBy(F.col("last4_max_new_group_flag").desc(), "id", "k")
w7 = Window.partitionBy("service").orderBy(F.col("island_id_avg_3").desc(), "id", "k")
w8 = Window.partitionBy("k", "service").orderBy("log_ts", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "v"),
    )
)

flagged = (
    flattened
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessions_app_logs
    .filter(F.col("new_group_flag") > 0)
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "v",
        "new_group_flag",
        F.max("new_group_flag").over(w_recent).alias("last4_max_new_group_flag"),
        F.min("new_group_flag").over(w_recent).alias("last4_min_new_group_flag"),
        F.avg("new_group_flag").over(w_recent).alias("last4_avg_new_group_flag"),
        F.sum("new_group_flag").over(w_recent).alias("last4_sum_new_group_flag"),
        F.avg("island_id").over(w4).alias("island_id_avg_3"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "v",
        "new_group_flag",
        "last4_max_new_group_flag",
        "island_id_avg_3",
        F.first("new_group_flag", ignorenulls=True).over(w5).alias("first_known_new_group_flag"),
        F.row_number().over(w6).alias("row_seq"),
        F.ntile(4).over(w7).alias("island_id_avg_3_bucket"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

valid = (
    scored
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .withColumn("island_id_avg_3_bucket_str", F.col("island_id_avg_3_bucket").cast("double"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("OTHER")))
)

result = (
    valid
    .groupBy("service")
    .pivot("k", ["trace", "version"])
    .agg(F.max("first_known_new_group_flag"))
)
