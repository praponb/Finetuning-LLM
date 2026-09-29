from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy(F.date_trunc("week", "event_time"), "event_type").orderBy(F.col("duration_sec").desc_nulls_first())
w3 = Window.partitionBy(F.year("event_time")).orderBy("duration_sec", "id")
w4 = Window.partitionBy("session_id")
w5 = Window.partitionBy("browser").orderBy(F.col("next_duration_sec_2").desc_nulls_first(), "id")

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-05-15 03:00:00").cast("timestamp"))
    .select(
        "*",
        F.lower("event_type").alias("event_type_lower"),
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

metrics = (
    base
    .filter(F.col("session_id").isin([16, 27]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "event_type_prefix",
        "page_filled",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
        F.lead("duration_sec", 2, 0).over(w3).alias("next_duration_sec_2"),
        F.min("duration_sec").over(w4).alias("session_id_min_duration_sec"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.next_duration_sec_2",
        "e.session_id_min_duration_sec",
        F.col("b.browser"),
        F.col("b.country"),
        F.col("b.device"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    ranked
    .filter(F.col("next_duration_sec_2") >= 467)
    .withColumn("device_title", F.initcap(F.trim("device")))
    .withColumn("device_filled", F.coalesce("device", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .cube(
        F.when(F.col("next_duration_sec_2") >= 462, "HIGH").otherwise("LOW").alias("next_duration_sec_2_tier"),
        F.when(F.col("session_id_min_duration_sec") >= 502, "PREMIUM").otherwise("STANDARD").alias("session_id_min_duration_sec_tier"),
        "browser",
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("session_id_min_duration_sec_tier").desc_nulls_last())
)
