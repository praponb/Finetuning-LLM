from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy(F.year("log_ts"), "attr_key").orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "attr_key",
        F.lead("status_code", 1, 0).over(w).alias("next_status_code"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
    )
)
