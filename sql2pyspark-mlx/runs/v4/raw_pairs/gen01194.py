from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy("duration_sec", "id")
w5 = Window.orderBy(F.col("duration_sec_sum_7d").desc(), "id")
w_part = Window.orderBy("duration_sec_max_30d", "id")

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-03 18:00:00").cast("timestamp")) & F.col("page").isNotNull() & F.col("page").isin(["/checkout", "/home", "/product"]))
    .select(
        "*",
        F.trim("event_type").alias("clean_event_type"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.max("duration_sec").over(w).alias("duration_sec_max_30d"),
        F.sum("duration_sec").over(w2).alias("duration_sec_sum_7d"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_30d"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.country"))
)

scored_web_events = (
    joined
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("event_seq", F.row_number().over(w_part))
)

joined_web_events = (
    scored_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.browser"),
        F.col("p.device"),
        F.col("p.country").alias("session_country"),
    )
)

enriched = (
    joined_web_events
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.browser").alias("session_browser"))
)

filtered = (
    enriched
    .filter(F.col("browser").isNotNull() & F.col("duration_sec_max_30d").between(136, 299))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "clean_event_type",
        "page_filled",
        "duration_sec_sum_7d",
        "duration_sec_avg_30d",
        "country",
        "row_seq",
        "seq_no",
        "event_seq",
        "browser",
        "session_country",
        "session_browser",
        F.when(F.col("event_seq") >= 7, "High").when(F.col("event_seq") >= 3, "Medium").otherwise("Low").alias("event_seq_band"),
        F.round((F.col("duration_sec_sum_7d") * 0.9), 2).alias("duration_sec_sum_7d_scaled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "session_id",
        "clean_event_type",
        "duration_sec_sum_7d",
        "duration_sec_avg_30d",
        "country",
        "row_seq",
        "seq_no",
        "event_seq",
        F.when(F.col("country").rlike("^(JP|SG)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|US)$"), "GROUP_A").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("country").asc_nulls_first(), F.col("country_zone").desc())
)
