from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.orderBy("latency_ms", "id", "k")
w3 = Window.partitionBy("k").orderBy(F.col("running_avg_latency_ms").desc_nulls_last())
w4 = Window.partitionBy("host").orderBy("running_sum_latency_ms", "id", "k")
w5 = Window.orderBy("running_sum_latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("k").orderBy(F.col("running_max_latency_ms").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

scored = (
    flattened
    .filter(F.col("level") == "WARN")
    .withColumn("running_min_latency_ms", F.min("latency_ms").over(w_running))
    .withColumn("running_avg_latency_ms", F.avg("latency_ms").over(w_running))
    .withColumn("running_sum_latency_ms", F.sum("latency_ms").over(w_running))
    .withColumn("running_max_latency_ms", F.max("latency_ms").over(w_running))
    .withColumn("next_latency_ms", F.lead("latency_ms", 1).over(w_running_2))
)

filtered = (
    scored
    .filter(F.col("k").isNotNull())
    .withColumn("latency_ms_rounded", F.round("latency_ms", 0))
    .withColumn("k_clean", F.coalesce("k", F.lit("OTHER")))
)

metrics = (
    filtered
    .select(
        "id",
        "latency_ms",
        "k",
        "running_max_latency_ms",
        "latency_ms_rounded",
        "k_clean",
        F.percent_rank().over(w3).alias("running_avg_latency_ms_percent_rank"),
        F.row_number().over(w4).alias("event_seq"),
        F.last("latency_ms", ignorenulls=True).over(w5).alias("last_known_latency_ms"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "latency_ms",
        "k",
        "running_max_latency_ms",
        "latency_ms_rounded",
        "event_seq",
        "last_known_latency_ms",
        F.when(F.col("k").rlike("^(version|trace)$"), "CORE").when(F.col("k").rlike("^(region)$"), "GROUP_A").otherwise("EXTENDED").alias("k_zone"),
    )
)
