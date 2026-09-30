from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("flag_bit2", "id", "k")
w2 = Window.orderBy("status_code_high", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy(F.col("status_code_high").asc_nulls_last())
w4 = Window.partitionBy("k").orderBy("status_code_high", "id", "k").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("k")
w6 = Window.partitionBy("k").orderBy(F.col("last_known_latency_ms").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("k").isin(["region", "trace"]))
    .select(
        "id",
        "host",
        "latency_ms",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

enriched = (
    flattened_attrs
    .filter(F.col("flag_bit2") == 0)
    .select(
        "id",
        "latency_ms",
        "k",
        "status_code_high",
        F.lead("status_code_high", 1, 0).over(w).alias("next_status_code_high"),
        F.last("latency_ms", ignorenulls=True).over(w2).alias("last_known_latency_ms"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "id",
        "k",
        "status_code_high",
        "next_status_code_high",
        "last_known_latency_ms",
        "dedup_rank",
        F.avg("status_code_high").over(w4).alias("status_code_high_avg_5"),
        F.sum("latency_ms").over(w5).alias("k_sum_latency_ms"),
    )
)

base = (
    scored
    .filter(F.col("k").isNotNull() & (F.col("last_known_latency_ms") < 1510))
    .select(
        "id",
        "k",
        "status_code_high",
        "next_status_code_high",
        "last_known_latency_ms",
        "dedup_rank",
        F.col("status_code_high").cast("double").alias("status_code_high_num"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

latest_app_logs = (
    base
    .withColumn("dedup_rank_2", F.rank().over(w6))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

result = (
    latest_app_logs
    .select(
        "id",
        "k",
        "last_known_latency_ms",
        "dedup_rank",
        "status_code_high_num",
        "resolved_k",
    )
)
