from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code_high", "id", "k")
w2 = Window.partitionBy("host", "k").orderBy("latest_rank", "id", "k")
w3 = Window.partitionBy("host", "k")
w4 = Window.partitionBy("host").orderBy("status_code", "id", "k")
w5 = Window.partitionBy("host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "host",
        "status_code",
        "k",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

latest = (
    flattened_attrs
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped
    .filter(F.col("flag_bit1") == 1)
    .select(
        "id",
        "host",
        "status_code",
        "k",
        "attr_value",
        "status_code_high",
        "flag_bit1",
        "rank_in_group",
        (F.col("rank_in_group") - F.col("flag_bit1")).alias("rank_in_group_minus_flag_bit1"),
    )
)

result = (
    base
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4))
    .withColumn("prev_flag_bit1", F.lag("flag_bit1", 1).over(w4))
    .withColumn("host_max_status_code_high", F.max("status_code_high").over(w5))
)
