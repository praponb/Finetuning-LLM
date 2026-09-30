from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id")
w4 = Window.orderBy("greatest_next_duration_sec_duration_sec", "id")
w5 = Window.orderBy("duration_sec", "id")

ranked = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "session_id", "page", "event_time", "duration_sec")
)

metrics = (
    ranked
    .select(
        "id",
        "duration_sec",
        F.lead("duration_sec").over(w2).alias("next_duration_sec"),
        F.lag("duration_sec").over(w2).alias("prev_duration_sec"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("duration_sec").over(w3).alias("session_id_avg_duration_sec"),
    )
)

cleaned = (
    metrics
    .filter(F.col("row_seq").between(5, 6))
    .select(
        "*",
        F.greatest("next_duration_sec", "duration_sec").alias("greatest_next_duration_sec_duration_sec"),
        F.when(F.col("next_duration_sec") >= 481, "High").when(F.col("next_duration_sec") >= 173, "Medium").otherwise("Low").alias("next_duration_sec_tier"),
    )
)

metrics_web_events = (
    cleaned
    .filter(F.col("greatest_next_duration_sec_duration_sec") < 80)
    .withColumn("next_prev_duration_sec", F.lead("prev_duration_sec", 2).over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

kept_keys = (
    metrics_web_events
    .select("id")
    .exceptAll(
        df_web_events
        .filter(F.to_date("event_time") < "2023-06-14")
        .select("id")
    )
)

kept_rows = (
    metrics_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    kept_rows
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("event_type") == "search")
        .select("id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("row_seq") <= 4)
    .select(
        "duration_sec",
        "next_duration_sec",
        "prev_duration_sec",
        "row_seq",
        "next_duration_sec_tier",
        "next_prev_duration_sec",
        "seq_no",
        (F.col("duration_sec") - F.col("greatest_next_duration_sec_duration_sec")).alias("duration_sec_minus_greatest_next_duration_sec_duration_sec"),
        F.col("duration_sec").cast("double").alias("duration_sec_str"),
    )
)

daily = (
    filtered
    .cube("seq_no")
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or_2"),
        F.count("*").alias("row_count"),
        F.round(F.avg("duration_sec_str"), 2).alias("avg_duration_sec_str"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or_3"),
    )
    .filter(F.col("duration_sec_bit_or_3") > 1)
)

result = (
    daily
    .select(
        "duration_sec_bit_or",
        "row_count",
        "avg_duration_sec_str",
        "duration_sec_bit_or_3",
    )
    .orderBy(F.col("duration_sec_bit_or_3").asc_nulls_last())
)
