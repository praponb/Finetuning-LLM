from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "k").orderBy(F.col("is_flagged").desc(), "id", "k")
w2 = Window.partitionBy("host", "k").orderBy(F.col("is_flagged").desc_nulls_last(), "id", "k")
w_hist = Window.partitionBy("host")
w4 = Window.partitionBy("k")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

flattened_attrs = (
    lines
    .select(
        "id",
        "host",
        "latency_ms",
        "k",
        "value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

cleaned = (
    flattened_attrs
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "host",
        "latency_ms",
        "k",
        "is_flagged",
        "status_code_high",
        F.col("status_code_high").cast("string").alias("status_code_high_num"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped_app_logs = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched_keys = (
    deduped_app_logs
    .select("host")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") == 997)
        .select("host")
    )
)

verified = (
    deduped_app_logs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "host",
        "k",
        "is_flagged",
        "status_code_high_num",
        "dedup_rank",
        F.sum("status_code_high").over(w_hist).alias("host_sum_status_code_high"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
        F.avg("is_flagged").over(w_hist).alias("host_avg_is_flagged"),
    )
)

result = (
    scored
    .select(
        "id",
        "host",
        "k",
        "dedup_rank",
        "latency_ms_share",
        F.when(F.col("k").rlike("^(trace|version)$"), "OTHER_ZONE").when(F.col("k").rlike("^(region)$"), "CORE").otherwise("GROUP_A").alias("k_zone"),
    )
)
