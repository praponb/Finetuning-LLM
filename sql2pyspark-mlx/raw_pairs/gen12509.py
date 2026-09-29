from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("flag_bit8", "id", "k").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("level", "k").orderBy("status_code_high")
w_ordered = Window.partitionBy("k").orderBy("flag_bit8", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k", "level").orderBy("flag_bit8_avg_5", "id", "k")
w5 = Window.partitionBy("k").orderBy(F.col("running_max_status_code_high_rank").desc_nulls_last(), "id", "k")
w6 = Window.partitionBy("k").orderBy(F.col("running_sum_status_code_high_rank").desc())

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
        F.explode("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .select(
        "id",
        "level",
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

scored = (
    flattened_attrs
    .select(
        "id",
        "level",
        "k",
        "attr_value",
        "flag_bit8",
        F.avg("flag_bit8").over(w).alias("flag_bit8_avg_5"),
        F.rank().over(w_running).alias("status_code_high_rank"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "k",
        "attr_value",
        "flag_bit8",
        "flag_bit8_avg_5",
        F.max("status_code_high_rank").over(w_ordered).alias("running_max_status_code_high_rank"),
        F.sum("status_code_high_rank").over(w_ordered).alias("running_sum_status_code_high_rank"),
        F.avg("status_code_high_rank").over(w_ordered).alias("running_avg_status_code_high_rank"),
        F.min("status_code_high_rank").over(w_ordered).alias("running_min_status_code_high_rank"),
        F.ntile(10).over(w4).alias("flag_bit8_avg_5_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    ranked
    .filter(F.col("flag_bit8").between(0, 1))
    .select(
        "*",
        (F.col("running_max_status_code_high_rank") - F.col("running_sum_status_code_high_rank")).alias("running_max_status_code_high_rank_minus_running_sum_status_code_high_rank"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

result = (
    filtered
    .select(
        "id",
        "k",
        "attr_value",
        "flag_bit8",
        "running_max_status_code_high_rank",
        "running_sum_status_code_high_rank",
        "running_min_status_code_high_rank",
        "row_num",
        "running_max_status_code_high_rank_minus_running_sum_status_code_high_rank",
        F.when(F.col("k").rlike("^(trace|region)$"), "GROUP_B").when(F.col("k").rlike("^(version)$"), "GROUP_A").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("k_zone").desc_nulls_last())
)
