from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code_high", "id", "key")
w2 = Window.orderBy(F.col("flag_bit8").desc_nulls_last())
w3 = Window.partitionBy("host").orderBy("flag_bit8", "id", "key").rowsBetween(-6, -1)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

flattened_attrs = (
    flattened
    .filter(F.col("status_code") == 330)
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "key",
        "value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "host",
        "status_code",
        "key",
        "value",
        "flag_bit8",
        "status_code_high",
        F.when(F.col("flag_bit8") >= 1, "High").when(F.col("flag_bit8") >= 0, "Medium").otherwise("Low").alias("flag_bit8_band"),
    )
)

with_windows = (
    base
    .withColumn("next_flag_bit8", F.lead("flag_bit8", 1).over(w))
    .withColumn("flag_bit8_rank", F.rank().over(w2))
    .withColumn("status_code_high_avg_6_prior", F.avg("status_code_high").over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "host",
        "status_code",
        "key",
        "flag_bit8",
        "flag_bit8_band",
        "next_flag_bit8",
        "flag_bit8_rank",
        "status_code_high_avg_6_prior",
        F.when(F.col("key").rlike("^(trace|version)$"), "EXTENDED").when(F.col("key").rlike("^(region)$"), "CORE").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy("flag_bit8_rank")
)
