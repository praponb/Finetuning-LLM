from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("flag_bit1").desc())
w_grp = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc())
w3 = Window.orderBy("status_code_high")
w4 = Window.orderBy(F.col("status_code_high").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

unpacked = (
    flattened
    .filter(F.col("attr_key") != "region")
    .select(
        "attr_key",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .select(
        "v",
        "status_code_high",
        F.percent_rank().over(w).alias("flag_bit1_percent_rank"),
        F.percent_rank().over(w_grp).alias("status_code_high_percent_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("status_code_high_percent_rank") <= 0.52)
    .select(
        "*",
        F.when(F.col("status_code_high_percent_rank") >= 0.63, "High").when(F.col("status_code_high_percent_rank") >= 0.22, "Medium").otherwise("Low").alias("status_code_high_percent_rank_level"),
    )
)

valid = (
    filtered
    .filter(F.col("flag_bit1_percent_rank") < 0.41)
    .select(
        "v",
        "status_code_high",
        "status_code_high_percent_rank",
        "status_code_high_percent_rank_level",
        (F.col("flag_bit1_percent_rank") * F.col("status_code_high")).alias("flag_bit1_percent_rank_times_status_code_high"),
        (F.col("flag_bit1_percent_rank") + F.col("status_code_high_percent_rank")).alias("flag_bit1_percent_rank_plus_status_code_high_percent_rank"),
    )
)

with_windows = (
    valid
    .select(
        "status_code_high_percent_rank_level",
        "flag_bit1_percent_rank_plus_status_code_high_percent_rank",
        F.rank().over(w3).alias("status_code_high_rank"),
        F.dense_rank().over(w4).alias("status_code_high_dense_rank"),
    )
)

valid_app_logs = (
    with_windows
    .filter(F.col("flag_bit1_percent_rank_plus_status_code_high_percent_rank").between(29, 59))
    .select(
        "*",
        F.round("status_code_high_dense_rank", 1).alias("status_code_high_dense_rank_rounded"),
    )
)

result = (
    valid_app_logs
    .withColumn("flag_bit1_percent_rank_plus_status_code_high_percent_rank_tier", F.when(F.col("flag_bit1_percent_rank_plus_status_code_high_percent_rank") >= 85, "PREMIUM").otherwise("STANDARD"))
    .withColumn("status_code_high_dense_rank_tier", F.when(F.col("status_code_high_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["flag_bit1_percent_rank_plus_status_code_high_percent_rank_tier", "status_code_high_dense_rank_tier"], ["flag_bit1_percent_rank_plus_status_code_high_percent_rank_tier"]],
        "flag_bit1_percent_rank_plus_status_code_high_percent_rank_tier",
        "status_code_high_dense_rank_tier",
    )
    .agg(
        F.round(F.avg("status_code_high_rank"), 2).alias("avg_status_code_high_rank"),
        F.max("status_code_high_rank").alias("max_status_code_high_rank"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("avg_status_code_high_rank").desc())
)
