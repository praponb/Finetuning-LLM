from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("level")
w2 = Window.partitionBy("level", "host").orderBy(F.col("has_fault").desc())
w3 = Window.partitionBy("host").orderBy("level_count_has_fault")
w_running = Window.partitionBy("level").orderBy(F.col("status_code_high").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

parsed = (
    flattened
    .select(
        "host",
        "level",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .withColumn("level_count_has_fault", F.count("has_fault").over(w_grp))
    .withColumn("has_fault_cume_dist", F.cume_dist().over(w2))
)

with_windows_app_logs = (
    with_windows
    .withColumn("level_count_has_fault_rank", F.rank().over(w3))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w_running))
)

result = (
    with_windows_app_logs
    .groupBy("level")
    .agg(
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
    .orderBy(F.col("num_ks").desc())
)
