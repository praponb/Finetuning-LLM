from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("log_ts"), "k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select("id", "host", "log_ts", "level", F.explode("context").alias("k", "value"))
)

with_windows = (
    flattened
    .select(
        "*",
        F.last("k", ignorenulls=True).over(w).alias("last_known_k"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
        F.last("k", ignorenulls=True).over(w3).alias("last_known_k_2"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w4).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w4) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("island_id", F.sum("is_new_session").over(w5))

cleaned = (
    sessions_app_logs
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & (F.col("seq_no") > 3))
    .withColumn("level_length", F.length("level"))
    .withColumn("island_id_band", F.when(F.col("island_id") >= 8, "High").when(F.col("island_id") >= 3, "Medium").otherwise("Low"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("N/A")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("island_id") >= 6, "HIGH").otherwise("LOW").alias("island_id_tier"),
        F.date_trunc("hour", "log_ts").alias("hour_window"),
        F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        F.max("level_length").alias("max_level_length"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.percentile_approx("level_length", 0.9).alias("level_length_p90"),
        F.countDistinct("host").alias("distinct_hosts"),
    )
    .filter(F.col("distinct_hosts") < 1000)
)
