from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("latency_ms").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "status_code",
        "attr_key",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.first("latency_ms", ignorenulls=True).over(w2).alias("first_known_latency_ms"),
        F.dense_rank().over(w_hist).alias("latency_ms_dense_rank"),
    )
)
