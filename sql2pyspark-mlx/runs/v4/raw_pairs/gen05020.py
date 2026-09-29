from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w_grp = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key")
w4 = Window.partitionBy("attr_key", "level").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "*",
        F.lead("status_code", 2).over(w).alias("next_status_code"),
        F.sum("status_code").over(w_grp).alias("running_status_code"),
        F.lead("latency_ms", 1).over(w3).alias("next_latency_ms"),
        F.last("latency_ms", ignorenulls=True).over(w4).alias("last_known_latency_ms"),
        F.percent_rank().over(w5).alias("status_code_percent_rank"),
    )
)
