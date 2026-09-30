from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts"), "key").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_ordered = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("host").orderBy("status_code", "id", "key")
w_hist = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .filter(F.col("log_ts") >= "2023-09-26")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        F.rank().over(w).alias("status_code_rank"),
        F.sum("status_code").over(w2).alias("status_code_sum_15m"),
        F.dense_rank().over(w_ordered).alias("status_code_dense_rank"),
        F.row_number().over(w4).alias("event_seq"),
        F.sum("status_code").over(w_hist).alias("host_sum_status_code"),
    )
)
