from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "k")
w2 = Window.partitionBy("host").orderBy("status_code_high", "id", "k")
w3 = Window.partitionBy("host", "k").orderBy(F.col("status_code_high").desc_nulls_last(), "id", "k")

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
        F.explode("context").alias("k", "v"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "k",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    flattened_attrs
    .withColumn("host_sum_flag_bit1", F.sum("flag_bit1").over(w))
    .withColumn("next_flag_bit1", F.lead("flag_bit1", 1, 0).over(w2))
)

base = (
    scored
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        (F.col("status_code_high") - F.col("host_sum_flag_bit1")).alias("status_code_high_minus_host_sum_flag_bit1"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .select(
        "id",
        "service",
        "host",
        "k",
        "flag_bit1",
        "status_code_high",
        "host_sum_flag_bit1",
        "next_flag_bit1",
        "status_code_high_minus_host_sum_flag_bit1",
        F.when(F.col("host").rlike("^(h4|h3)$"), "GROUP_A").when(F.col("host").rlike("^(h1|h2)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("host_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
