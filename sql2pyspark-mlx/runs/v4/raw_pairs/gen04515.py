from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("k").orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.orderBy(F.col("status_code_high").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "k",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .select(
        "*",
        F.stddev_samp("is_flagged").over(w).alias("k_stddev_samp_is_flagged"),
        F.sum("status_code_high").over(w).alias("k_sum_status_code_high"),
        F.percent_rank().over(w2).alias("status_code_high_percent_rank"),
        F.rank().over(w3).alias("status_code_high_rank"),
    )
)

totals = (
    with_windows
    .groupBy("k")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.max(F.when(F.col("k") == "version", F.col("is_flagged"))).alias("version_max_is_flagged"),
    )
    .filter(F.col("version_max_is_flagged") >= 0)
)

result = (
    totals
    .select(
        "k",
        "cnt",
        "k_list",
        "num_ks",
        F.when(F.col("k").rlike("^(version|region)$"), "OTHER_ZONE").when(F.col("k").rlike("^(trace)$"), "EXTENDED").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
