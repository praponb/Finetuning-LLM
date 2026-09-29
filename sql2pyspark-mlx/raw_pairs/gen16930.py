from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("log_ts") > F.lit("2025-06-01 14:00:00").cast("timestamp"))
    .withColumn("status_code_str", F.col("status_code").cast("double"))
)

cleaned = (
    filtered
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & (~F.col("k").eqNullSafe(F.lit("region"))))
    .select(
        "id",
        "level",
        "status_code",
        "k",
        "value",
        F.substring("level", 1, 3).alias("level_prefix"),
        F.concat_ws("-", "k", "level").alias("k_level_key"),
    )
)

valid = (
    cleaned
    .filter(F.col("status_code") >= 398)
    .select(
        "id",
        "level",
        "status_code",
        "k",
        "level_prefix",
        "k_level_key",
        F.initcap(F.trim("level")).alias("level_title"),
    )
)

scored = (
    valid
    .withColumn("running_min_status_code", F.min("status_code").over(w_ordered))
    .withColumn("running_avg_status_code", F.avg("status_code").over(w_ordered))
    .withColumn("running_sum_status_code", F.sum("status_code").over(w_ordered))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w2))
)

result = (
    scored
    .groupBy("level")
    .agg(
        F.count_if(F.col("k") == "region").alias("region_events"),
        F.sum(F.when(F.col("level") == "INFO", F.col("cumulative_status_code")).otherwise(0)).alias("info_cumulative_status_code"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .filter(F.col("region_events") < 1000)
    .orderBy(F.col("num_levels").desc_nulls_last())
)
