from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").desc(), "id", "k")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy("island_id", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("k", "value"),
    )
)

latest = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    latest
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "k",
        "value",
        F.concat_ws("-", "k", "level").alias("k_level_key"),
    )
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w3))

with_windows = (
    islands
    .filter(F.col("level").isin(["WARN", "INFO", "DEBUG"]))
    .select(
        "*",
        F.max("island_id").over(w4).alias("island_id_max_15m"),
        F.sum("island_id").over(w5).alias("cumulative_island_id"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
    )
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.countDistinct("level").alias("distinct_levels"),
        F.percentile_approx("cumulative_island_id", 0.9).alias("cumulative_island_id_p90"),
    )
    .filter(F.col("cumulative_island_id_p90") < 1000000)
)
