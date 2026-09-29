from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(-2, -1)
w3 = Window.partitionBy("key")
w4 = Window.partitionBy("key").orderBy("running_sum_status_code_int", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

enriched = (
    flattened
    .filter(F.to_date("log_ts") >= "2023-07-30")
    .select(
        "id",
        "key",
        "v",
        F.max("status_code").over(w_part).alias("running_max_status_code"),
        F.avg("status_code").over(w_part).alias("running_avg_status_code"),
        F.sum("status_code").over(w_part).alias("running_sum_status_code"),
        F.count("status_code").over(w_part).alias("running_count_status_code"),
        F.avg("status_code").over(w2).alias("status_code_avg_2_prior"),
    )
)

valid = (
    enriched
    .filter(F.col("running_avg_status_code") < 280)
    .select(
        "id",
        "key",
        "v",
        "running_avg_status_code",
        "running_sum_status_code",
        "status_code_avg_2_prior",
        (F.col("running_count_status_code") - F.col("running_avg_status_code")).alias("running_count_status_code_minus_running_avg_status_code"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_clean"),
    )
)

filtered = (
    valid
    .filter(F.col("running_sum_status_code").between(800, 1600))
    .select(
        "id",
        "key",
        "v",
        "running_avg_status_code",
        "status_code_avg_2_prior",
        F.col("running_sum_status_code").cast("int").alias("running_sum_status_code_int"),
        (F.col("running_avg_status_code") / F.nullif("running_count_status_code_minus_running_avg_status_code", F.lit(0))).alias("running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

metrics = (
    filtered
    .withColumn("key_max_running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code", F.max("running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code").over(w3))
    .withColumn("prev_running_avg_status_code", F.lag("running_avg_status_code", 2).over(w4))
    .withColumn("running_avg_status_code_share", F.round((F.col("running_avg_status_code") / F.sum("running_avg_status_code").over(w3)), 4))
)

valid_app_logs = (
    metrics
    .filter(F.col("key").isNotNull() & (F.col("key").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "key",
        "v",
        "running_avg_status_code",
        "status_code_avg_2_prior",
        "running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code",
        "key_filled",
        "running_avg_status_code_share",
        F.floor("status_code_avg_2_prior").alias("status_code_avg_2_prior_whole"),
        (F.col("prev_running_avg_status_code") * F.col("running_sum_status_code_int")).alias("prev_running_avg_status_code_times_running_sum_status_code_int"),
    )
)

filtered_app_logs = (
    valid_app_logs
    .filter(F.col("key").isNotNull() & (F.col("status_code_avg_2_prior_whole") > 29))
    .select(
        "id",
        "key",
        "v",
        "running_avg_status_code",
        "status_code_avg_2_prior",
        "running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code",
        "key_filled",
        "status_code_avg_2_prior_whole",
        F.col("running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code").cast("int").alias("running_avg_status_code_per_running_count_status_code_minus_running_avg_status_code_int"),
    )
)

result = (
    filtered_app_logs
    .select("id", "key", "v", "status_code_avg_2_prior", "key_filled")
    .orderBy(F.col("key").desc_nulls_last(), "status_code_avg_2_prior")
)
