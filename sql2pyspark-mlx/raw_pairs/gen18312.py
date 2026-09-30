from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("service").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "key",
        "v",
        F.min("status_code").over(w_part).alias("running_min_status_code"),
        F.avg("status_code").over(w_part).alias("running_avg_status_code"),
        F.count("status_code").over(w_part).alias("running_count_status_code"),
    )
)
