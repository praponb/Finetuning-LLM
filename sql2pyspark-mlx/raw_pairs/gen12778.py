from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec")
w2 = Window.orderBy(F.col("least_duration_sec_scaled_duration_sec").asc_nulls_first())
w3 = Window.orderBy(F.col("duration_sec_plus_duration_sec_scaled").desc())

remaining_keys = (
    df_web_events
    .select("session_id", "id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2025-03-01")
        .select("session_id", "id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("duration_sec") > 359)
    .select(
        "id",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.upper("page").alias("page_upper"),
    )
)

cleaned = (
    base
    .filter(F.col("duration_sec") <= 399)
    .withColumn("duration_sec_scaled", F.round((F.col("duration_sec") * 1.1), 2))
)

valid = (
    cleaned
    .filter(F.col("duration_sec").between(436, 437))
    .select(
        "*",
        (F.col("duration_sec") / F.nullif("duration_sec_scaled", F.lit(0))).alias("duration_sec_per_duration_sec_scaled"),
        F.col("duration_sec").cast("double").alias("duration_sec_str"),
    )
)

valid_web_events = (
    valid
    .filter(F.col("duration_sec_str").between(35, 58))
    .select(
        "id",
        "duration_sec",
        "event_time_label",
        "duration_sec_scaled",
        "duration_sec_per_duration_sec_scaled",
        (F.col("duration_sec") + F.col("duration_sec_scaled")).alias("duration_sec_plus_duration_sec_scaled"),
        F.least("duration_sec_scaled", "duration_sec").alias("least_duration_sec_scaled_duration_sec"),
    )
)

enriched = valid_web_events.select("*", F.rank().over(w).alias("duration_sec_rank"))

enriched_web_events = (
    enriched
    .withColumn("least_duration_sec_scaled_duration_sec_dense_rank", F.dense_rank().over(w2))
    .withColumn("duration_sec_plus_duration_sec_scaled_dense_rank", F.dense_rank().over(w3))
)

totals = (
    enriched_web_events
    .withColumn("duration_sec_per_duration_sec_scaled_tier", F.when(F.col("duration_sec_per_duration_sec_scaled") >= 81, "LARGE").otherwise("SMALL"))
    .withColumn("duration_sec_plus_duration_sec_scaled_dense_rank_tier", F.when(F.col("duration_sec_plus_duration_sec_scaled_dense_rank") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("duration_sec_rank_tier", F.when(F.col("duration_sec_rank") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["duration_sec_per_duration_sec_scaled_tier", "duration_sec_plus_duration_sec_scaled_dense_rank_tier", "duration_sec_rank_tier"], ["duration_sec_per_duration_sec_scaled_tier", "duration_sec_plus_duration_sec_scaled_dense_rank_tier"], ["duration_sec_per_duration_sec_scaled_tier"]],
        "duration_sec_per_duration_sec_scaled_tier",
        "duration_sec_plus_duration_sec_scaled_dense_rank_tier",
        "duration_sec_rank_tier",
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.avg("least_duration_sec_scaled_duration_sec").alias("avg_least_duration_sec_scaled_duration_sec"),
        F.avg("duration_sec_plus_duration_sec_scaled_dense_rank").alias("avg_duration_sec_plus_duration_sec_scaled_dense_rank"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select(
        "duration_sec_per_duration_sec_scaled_tier",
        "duration_sec_bit_or",
        "avg_least_duration_sec_scaled_duration_sec",
        "avg_duration_sec_plus_duration_sec_scaled_dense_rank",
        "grouping_id",
    )
)
