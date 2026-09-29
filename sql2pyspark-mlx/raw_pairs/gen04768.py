from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("session_id", "device").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device").orderBy(F.col("last3_sum_duration_sec").asc_nulls_first())

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-07-26").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.when(F.col("duration_sec") >= 414, "High").when(F.col("duration_sec") >= 188, "Medium").otherwise("Low").alias("duration_sec_level"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .filter(F.col("d.browser").isin(["Edge", "Firefox", "Chrome"]))
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.event_time",
        "src.duration_sec",
        "src.duration_sec_rounded",
        "src.duration_sec_level",
        F.col("d.browser"),
        F.col("d.device"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("duration_sec").over(w_recent).alias("last3_sum_duration_sec"),
        F.min("duration_sec").over(w_recent).alias("last3_min_duration_sec"),
        F.count("duration_sec").over(w_recent).alias("last3_count_duration_sec"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    latest
    .select("id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") >= 362)
        .select("id", "event_type")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

wide = (
    reconciled
    .groupBy("device", "event_type")
    .pivot("browser", ["Chrome", "Firefox", "Edge"])
    .agg(F.avg("last3_min_duration_sec").alias("amount"))
)

result = (
    wide
    .select(
        "device",
        "event_type",
        "Edge",
        F.when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "GROUP_B").when(F.col("event_type").rlike("^(click|purchase)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("event_type_zone"),
    )
    .orderBy(F.col("device").desc_nulls_last())
)
