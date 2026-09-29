from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key")
w3 = Window.orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

base = (
    lines
    .filter(F.col("key").isNotNull() & (F.col("key") == "region"))
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 444, "High").when(F.col("status_code") >= 243, "Medium").otherwise("Low"))
)

with_windows = (
    base
    .select(
        "*",
        F.sum("status_code").over(w).alias("running_status_code"),
        F.max("status_code").over(w2).alias("key_max_status_code"),
        F.sum("status_code").over(w3).alias("running_status_code_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "level",
        "status_code",
        "key",
        "v",
        "status_code_tier",
        "running_status_code",
        "key_max_status_code",
        (F.col("status_code") * F.col("key_max_status_code")).alias("status_code_times_key_max_status_code"),
        (F.col("status_code") * F.col("running_status_code")).alias("status_code_times_running_status_code"),
    )
)

result = (
    valid
    .withColumn("key_max_status_code_tier", F.when(F.col("key_max_status_code") >= 441, "HIGH").otherwise("LOW"))
    .rollup("key_max_status_code_tier")
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy(F.col("row_count").desc())
)
