from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("log_ts").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("latest_rank", "id", "key")
w3 = Window.orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("latest_rank", "id", "key").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("latest_rank", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

valid = (
    exploded
    .filter(F.col("key").isin(["region", "trace"]))
    .select(
        "id",
        "log_ts",
        "key",
        "attr_value",
        F.lower("host").alias("host_lower"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "id",
        "key",
        "attr_value",
        "host_lower",
        "latest_rank",
        F.row_number().over(w2).alias("row_seq"),
        F.first("key", ignorenulls=True).over(w3).alias("first_known_key"),
    )
)

scored_app_logs = (
    scored
    .filter(F.col("row_seq").isNull())
    .select(
        "*",
        F.count("row_seq").over(w4).alias("last3_count_row_seq"),
        F.avg("row_seq").over(w4).alias("last3_avg_row_seq"),
        F.max("row_seq").over(w4).alias("last3_max_row_seq"),
        F.lag("row_seq", 1).over(w5).alias("prev_row_seq"),
    )
)

result = (
    scored_app_logs
    .groupBy(
        "key",
        F.when(F.col("prev_row_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("prev_row_seq_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.sum(F.when(F.col("key") == "version", F.col("row_seq"))).alias("version_total_row_seq"),
        F.percentile_approx("last3_count_row_seq", 0.95).alias("last3_count_row_seq_p95"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.sum(F.when(F.col("key") == "region", F.col("last3_count_row_seq")).otherwise(0)).alias("region_last3_count_row_seq"),
    )
)
