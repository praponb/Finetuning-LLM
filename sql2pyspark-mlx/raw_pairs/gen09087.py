from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "attr_key").orderBy(F.col("status_code_high").desc_nulls_first(), "id", "attr_key")
w2 = Window.partitionBy("attr_key", "service").orderBy("flag_bit1", "id", "attr_key").rowsBetween(-6, -1)
w3 = Window.orderBy("status_code_high", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w5 = Window.partitionBy("attr_key", "service").orderBy("row_num", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("service", "attr_key").orderBy("row_num", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("service").orderBy("latency_ms_avg_6", "id", "attr_key")
w8 = Window.partitionBy("attr_key", "service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "service",
        "latency_ms",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

ranked = (
    parsed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "service",
        "attr_key",
        "attr_value",
        "status_code_high",
        "row_num",
        F.avg("status_code_high").over(w2).alias("status_code_high_avg_6_prior"),
        F.sum("status_code_high").over(w3).alias("running_status_code_high"),
        F.lag("flag_bit1", 1).over(w4).alias("prev_flag_bit1"),
        F.last("attr_key", ignorenulls=True).over(w5).alias("last_known_attr_key"),
        F.avg("latency_ms").over(w6).alias("latency_ms_avg_6"),
    )
)

result = (
    scored
    .filter(F.col("latency_ms_avg_6") == 876)
    .select(
        "id",
        "attr_key",
        "row_num",
        "last_known_attr_key",
        "latency_ms_avg_6",
        F.row_number().over(w_running).alias("event_seq"),
        F.sum("prev_flag_bit1").over(w3).alias("running_prev_flag_bit1"),
        F.sum("status_code_high_avg_6_prior").over(w8).alias("attr_key_sum_status_code_high_avg_6_prior"),
    )
)
