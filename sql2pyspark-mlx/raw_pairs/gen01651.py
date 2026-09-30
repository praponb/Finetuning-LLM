from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("host", "attr_key").orderBy(F.col("latency_ms").desc())
w3 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc_nulls_first())
w4 = Window.partitionBy(F.year("log_ts"), "host")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "latency_ms",
        "attr_key",
        "attr_value",
        F.count("status_code").over(w).alias("status_code_count_1h"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.rank().over(w3).alias("latency_ms_rank_2"),
        F.avg("latency_ms").over(w4).alias("log_ts_year_avg_latency_ms"),
    )
)
