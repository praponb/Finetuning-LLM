from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id", "attr_key").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy("flag_bit4", "id", "attr_key")
w3 = Window.partitionBy("attr_key", "service").orderBy(F.col("flag_bit4_avg_5").desc_nulls_last(), "id", "attr_key")
w4 = Window.partitionBy("attr_key").orderBy("flag_bit4", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("attr_key", "level")
w6 = Window.partitionBy("attr_key", "level").orderBy("flag_bit4", "id", "attr_key")
w7 = Window.partitionBy("level", "attr_key").orderBy(F.col("flag_bit4_avg_5").desc_nulls_last())
w8 = Window.orderBy(F.col("flag_bit4").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "attr_key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    flattened_attrs
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        "flag_bit4",
        F.avg("flag_bit4").over(w).alias("flag_bit4_avg_5"),
        F.ntile(5).over(w2).alias("flag_bit4_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "level", "attr_key", "flag_bit4", "flag_bit4_avg_5")
)

metrics = (
    latest
    .select(
        "id",
        "level",
        "attr_key",
        F.avg("flag_bit4_avg_5").over(w4).alias("flag_bit4_avg_5_avg_3"),
        F.avg("flag_bit4").over(w5).alias("attr_key_avg_flag_bit4"),
        F.lead("flag_bit4_avg_5", 2, 0).over(w6).alias("next_flag_bit4_avg_5"),
        F.percent_rank().over(w7).alias("flag_bit4_avg_5_percent_rank"),
        F.dense_rank().over(w8).alias("flag_bit4_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "attr_key",
        "flag_bit4_avg_5_avg_3",
        "attr_key_avg_flag_bit4",
        "next_flag_bit4_avg_5",
        "flag_bit4_avg_5_percent_rank",
        "flag_bit4_dense_rank",
    )
    .orderBy(F.col("flag_bit4_avg_5_avg_3").asc_nulls_last())
)
