from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("key", "level").orderBy("status_code", "id", "key").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("host", "key")
w3 = Window.partitionBy("key").orderBy("last7_avg_status_code")
w4 = Window.partitionBy("key").orderBy("last7_avg_status_code", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

metrics = (
    exploded
    .select(
        "id",
        "key",
        "attr_value",
        F.avg("status_code").over(w_hist).alias("last7_avg_status_code"),
        F.max("status_code").over(w_hist).alias("last7_max_status_code"),
        F.sum("status_code").over(w_hist).alias("last7_sum_status_code"),
        F.sum("status_code").over(w2).alias("host_sum_status_code"),
    )
)

base = (
    metrics
    .filter(F.col("key").isNotNull() & (F.col("last7_avg_status_code") < 350))
    .select(
        "id",
        "key",
        "attr_value",
        "last7_avg_status_code",
        "last7_max_status_code",
        "host_sum_status_code",
        (F.col("last7_max_status_code") - F.col("host_sum_status_code")).alias("last7_max_status_code_minus_host_sum_status_code"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

with_windows = (
    base
    .filter(F.col("last7_max_status_code_minus_host_sum_status_code").isNotNull())
    .select(
        "id",
        "key",
        "last7_avg_status_code",
        "last7_max_status_code",
        "key_filled",
        F.cume_dist().over(w3).alias("last7_avg_status_code_cume_dist"),
        F.lag("last7_max_status_code_minus_host_sum_status_code", 1).over(w4).alias("prev_last7_max_status_code_minus_host_sum_status_code"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "key",
        "last7_avg_status_code",
        "last7_max_status_code",
        "key_filled",
        "last7_avg_status_code_cume_dist",
    )
)
