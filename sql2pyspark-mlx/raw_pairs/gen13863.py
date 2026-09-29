from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "log_ts"), "service").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").desc(), "id", "key")
w4 = Window.partitionBy("key").orderBy("seq_no", "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("seq_no").desc(), "id", "key")
w6 = Window.partitionBy("key").orderBy("next_seq_no", "id", "key")
w7 = Window.partitionBy("key").orderBy(F.col("event_seq").desc())

exploded = (
    df_app_logs
    .select("id", "service", "log_ts", F.explode("context").alias("key", "value"))
)

metrics = (
    exploded
    .select(
        "id",
        "key",
        F.first("key", ignorenulls=True).over(w).alias("first_known_key"),
        F.last("key", ignorenulls=True).over(w2).alias("last_known_key"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

metrics_app_logs = (
    metrics
    .withColumn("next_seq_no", F.lead("seq_no").over(w4))
    .withColumn("prev_seq_no", F.lag("seq_no").over(w4))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

metrics_app_logs_2 = (
    metrics_app_logs
    .filter(F.col("key").isin(["version", "region", "trace"]))
    .withColumn("prev_row_seq", F.lag("row_seq").over(w6))
    .withColumn("next_next_seq_no", F.lead("next_seq_no").over(w6))
    .withColumn("row_seq_2", F.row_number().over(w6))
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w7))
)

base = (
    metrics_app_logs_2
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "key",
        "seq_no",
        "next_seq_no",
        "prev_seq_no",
        "row_seq",
        "event_seq",
        "next_next_seq_no",
        "row_seq_2",
        "event_seq_dense_rank",
        F.col("row_seq_2").cast("double").alias("row_seq_2_str"),
        F.round((F.col("prev_row_seq") / F.col("event_seq_dense_rank")), 2).alias("prev_row_seq_per_event_seq_dense_rank"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

result = (
    base
    .groupBy("key")
    .agg(
        F.percentile_approx("row_seq_2_str", [0.25, 0.5, 0.75]).alias("row_seq_2_str_quartiles"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
    )
    .orderBy(F.col("trace_count").asc_nulls_last())
)
