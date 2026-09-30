from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w_running = Window.orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("log_ts").desc(), "id", "k")
w_hist = Window.partitionBy("host").orderBy("log_ts", "id", "k")
w6 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("month", "log_ts")).orderBy(F.col("row_seq").asc_nulls_last())
w8 = Window.partitionBy(F.to_date("log_ts"), "k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w9 = Window.orderBy(F.col("row_seq").desc())
w10 = Window.partitionBy("service", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_ordered = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w13 = Window.partitionBy("k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode("context").alias("k", "attr_value"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.first("k", ignorenulls=True).over(w_running).alias("first_known_k"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

matched_keys = (
    enriched
    .select("id", "service")
    .subtract(
        df_app_logs
        .filter(F.col("service") == "auth")
        .select("id", "service")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("row_seq_2_avg_30m", F.avg("row_seq_2").over(w6))
    .withColumn("row_seq_dense_rank", F.dense_rank().over(w7))
    .withColumn("row_seq_2_max_15m", F.max("row_seq_2").over(w8))
    .withColumn("row_seq_percent_rank", F.percent_rank().over(w9))
    .withColumn("event_seq_avg_15m", F.avg("event_seq").over(w10))
)

enriched_app_logs = (
    scored
    .select(
        "service",
        "host",
        "attr_value",
        "row_seq_2",
        "first_known_k",
        "event_seq",
        "row_seq_2_avg_30m",
        "row_seq_percent_rank",
        "event_seq_avg_15m",
        F.sum("event_seq").over(w_ordered).alias("event_seq_sum_15m"),
        F.max("event_seq_avg_15m").over(w_part).alias("event_seq_avg_15m_max_2h"),
        F.rank().over(w9).alias("row_seq_rank"),
        F.count("event_seq").over(w_part).alias("event_seq_count_2h"),
        F.avg("row_seq").over(w13).alias("k_avg_row_seq"),
    )
)

stats = (
    enriched_app_logs
    .groupBy(
        F.when(F.col("event_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.max(F.when(F.col("service") == "billing", F.col("event_seq"))).alias("billing_max_event_seq"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.countDistinct("host").alias("distinct_hosts_2"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
)

result = (
    stats
    .select("event_seq_tier", "billing_max_event_seq", "distinct_hosts", "num_hosts")
    .orderBy("event_seq_tier", "num_hosts")
)
