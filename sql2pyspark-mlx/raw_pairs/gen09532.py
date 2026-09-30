from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code_high_whole").desc_nulls_first(), "id", "attr_key")
w_running = Window.partitionBy("level").orderBy(F.col("status_code_high_whole").desc(), "id", "attr_key")
w_running_2 = Window.partitionBy(F.date_trunc("week", "log_ts"), "level").orderBy(F.col("status_code_high").asc_nulls_first())
w4 = Window.orderBy("status_code_high_dense_rank", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

parsed = (
    flattened
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    parsed
    .filter((F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 07:00:00"))) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "log_ts",
        "level",
        "attr_key",
        "has_fault",
        "status_code_high",
        F.ceil("status_code_high").alias("status_code_high_whole"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "attr_key",
        F.ntile(3).over(w_running).alias("status_code_high_whole_bucket"),
        F.dense_rank().over(w_running_2).alias("status_code_high_dense_rank"),
    )
)

metrics = (
    with_windows
    .filter(F.col("status_code_high_dense_rank") > 7)
    .select(
        "*",
        F.first("status_code_high_dense_rank", ignorenulls=True).over(w4).alias("first_known_status_code_high_dense_rank"),
        F.min("status_code_high_dense_rank").over(w5).alias("attr_key_min_status_code_high_dense_rank"),
        F.last("status_code_high_dense_rank", ignorenulls=True).over(w4).alias("last_known_status_code_high_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "attr_key",
        "status_code_high_whole_bucket",
        "status_code_high_dense_rank",
        "first_known_status_code_high_dense_rank",
        "attr_key_min_status_code_high_dense_rank",
        "last_known_status_code_high_dense_rank",
        F.when(F.col("attr_key").rlike("^(version|region)$"), "CORE").when(F.col("attr_key").rlike("^(trace)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("attr_key_zone"),
    )
    .orderBy(F.col("attr_key_min_status_code_high_dense_rank").desc())
)
