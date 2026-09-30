from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "service").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "attr_key")
w2 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "attr_key")
w3 = Window.partitionBy("level", "service").orderBy("latest_rank_2", "id", "attr_key")
w_running = Window.partitionBy("level").orderBy("latest_rank", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("latest_rank", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level").orderBy(F.col("running_count_latency_ms").asc_nulls_first(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

latest = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_app_logs = (
    latest
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
)

ranked = (
    latest_app_logs
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked
    .select(
        "*",
        F.count("latency_ms").over(w_running).alias("running_count_latency_ms"),
        F.avg("latency_ms").over(w_running).alias("running_avg_latency_ms"),
        F.sum("latency_ms").over(w_running).alias("running_sum_latency_ms"),
        F.sum("latency_ms").over(w5).alias("running_latency_ms"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("running_latency_ms") >= 10536, "PREMIUM").otherwise("STANDARD").alias("running_latency_ms_tier"),
        F.when(F.col("running_avg_latency_ms") >= 2630, "PREMIUM").otherwise("STANDARD").alias("running_avg_latency_ms_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
    .orderBy(F.col("running_avg_latency_ms_tier").asc_nulls_first())
)
