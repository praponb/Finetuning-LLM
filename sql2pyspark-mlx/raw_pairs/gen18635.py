from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("host").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("host", "k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k")
w4 = Window.orderBy(F.col("cumulative_status_code").desc_nulls_last())
w5 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("host", "k").orderBy(F.col("cumulative_status_code_share").desc(), "id", "k")
w7 = Window.partitionBy("k").orderBy(F.col("cumulative_status_code").desc_nulls_first())
w8 = Window.partitionBy("host", "k")
w9 = Window.partitionBy("level").orderBy("status_code", "id", "k").rowsBetween(-6, -1)
w10 = Window.partitionBy("hour_window").orderBy(F.col("running_status_code_per_debug").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

scored = (
    exploded
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "k",
        "attr_value",
        F.sum("status_code").over(w_hist).alias("cumulative_status_code"),
        F.sum("status_code").over(w_running).alias("running_status_code"),
    )
)

with_windows = (
    scored
    .filter(F.col("host").isin(["h1", "h2", "h4"]))
    .withColumn("k_sum_running_status_code", F.sum("running_status_code").over(w3))
    .withColumn("cumulative_status_code_share", F.round((F.col("cumulative_status_code") / F.sum("cumulative_status_code").over(w3)), 4))
    .withColumn("cumulative_status_code_dense_rank", F.dense_rank().over(w4))
    .withColumn("cumulative_running_status_code", F.sum("running_status_code").over(w5))
)

metrics = (
    with_windows
    .select(
        "id",
        "log_ts",
        "level",
        "k",
        "cumulative_status_code",
        "running_status_code",
        "k_sum_running_status_code",
        "cumulative_status_code_dense_rank",
        F.ntile(4).over(w6).alias("cumulative_status_code_share_bucket"),
        F.cume_dist().over(w7).alias("cumulative_status_code_cume_dist"),
        F.round((F.col("cumulative_running_status_code") / F.sum("cumulative_running_status_code").over(w8)), 4).alias("cumulative_running_status_code_share"),
        F.avg("cumulative_status_code").over(w9).alias("cumulative_status_code_avg_6_prior"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("cumulative_status_code_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("cumulative_status_code_dense_rank_tier"),
        F.when(F.col("cumulative_status_code") >= 2195, "PREMIUM").otherwise("STANDARD").alias("cumulative_status_code_tier"),
    )
    .agg(
        F.count_if(F.col("level") == "INFO").alias("info_events"),
        F.count("*").alias("event_count"),
        F.round(F.sum("running_status_code") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("running_status_code_per_debug"),
    )
    .withColumn("running_status_code_per_debug_percent_rank", F.percent_rank().over(w10))
    .orderBy(F.col("cumulative_status_code_dense_rank_tier").desc_nulls_last())
)
