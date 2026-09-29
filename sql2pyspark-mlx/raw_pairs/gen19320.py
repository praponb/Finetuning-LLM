from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("status_code").desc(), "id", "key")
w_grp_2 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key")
w_ordered = Window.partitionBy("key").orderBy("cumulative_status_code_high", "id", "key")
w_hist = Window.partitionBy("key").orderBy("status_code_bucket")
w6 = Window.partitionBy("key").orderBy(F.col("status_code_bucket").asc_nulls_last(), "id", "key")
w7 = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.hour("log_ts").between(8, 16))
    .select(
        "id",
        "log_ts",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    flattened_attrs
    .select(
        "id",
        "status_code",
        "key",
        F.ntile(10).over(w_grp).alias("status_code_bucket"),
        F.sum("status_code_high").over(w_grp_2).alias("cumulative_status_code_high"),
        F.first("key", ignorenulls=True).over(w_grp_2).alias("first_known_key"),
    )
)

with_windows = (
    enriched
    .filter(F.col("status_code") >= 267)
    .select(
        "id",
        "status_code",
        "key",
        "status_code_bucket",
        "first_known_key",
        F.avg("status_code").over(w3).alias("key_avg_status_code"),
        F.lag("cumulative_status_code_high", 1).over(w_ordered).alias("prev_cumulative_status_code_high"),
        F.sum("status_code_bucket").over(w_grp_2).alias("running_status_code_bucket"),
        F.rank().over(w_hist).alias("status_code_bucket_rank"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    latest
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("version"))))
    .select(
        "*",
        (F.col("status_code_bucket_rank") * F.col("key_avg_status_code")).alias("status_code_bucket_rank_times_key_avg_status_code"),
        (F.col("status_code") - F.col("running_status_code_bucket")).alias("status_code_minus_running_status_code_bucket"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .rollup(
        F.when(F.col("prev_cumulative_status_code_high") >= 10, "PREMIUM").otherwise("STANDARD").alias("prev_cumulative_status_code_high_tier"),
        F.when(F.col("status_code_bucket") >= 7, "LARGE").otherwise("SMALL").alias("status_code_bucket_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.avg(F.when(F.col("key") == "trace", F.col("status_code_bucket_rank"))).alias("trace_avg_status_code_bucket_rank"),
        F.round(F.avg("status_code_bucket_rank_times_key_avg_status_code"), 2).alias("avg_status_code_bucket_rank_times_key_avg_status_code"),
        F.sum(F.when(F.col("key") == "trace", F.col("running_status_code_bucket")).otherwise(0)).alias("trace_running_status_code_bucket"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
    )
    .orderBy(F.col("status_code_bucket_tier").desc_nulls_last())
)
