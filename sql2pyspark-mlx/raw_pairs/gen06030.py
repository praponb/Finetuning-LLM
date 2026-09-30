from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("rn", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("is_new_session", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("event_type").orderBy("event_time", "id")
w8 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(~F.col("event_type").isin(["click", "purchase", "add_to_cart"]) & (F.col("event_time") > F.to_timestamp(F.lit("2023-04-05 02:00:00"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.hour("event_time").alias("event_time_hour"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped_web_events = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

flagged = (
    deduped_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w4))

metrics = (
    islands
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_time_hour",
        "is_new_session",
        "island_id",
        F.min("island_id").over(w5).alias("last4_min_island_id"),
        F.sum("island_id").over(w5).alias("last4_sum_island_id"),
        F.count("island_id").over(w5).alias("last4_count_island_id"),
        F.max("island_id").over(w5).alias("last4_max_island_id"),
        F.first("event_time_hour", ignorenulls=True).over(w6).alias("first_known_event_time_hour"),
    )
)

with_gaps = (
    metrics
    .withColumn("is_new_session_2", F.when(F.lag("event_time").over(w7).isNull() | (F.col("event_time") > F.lag("event_time").over(w7) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands_web_events = with_gaps.withColumn("island_id_2", F.sum("is_new_session_2").over(w8))

with_dims = (
    islands_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.started_at") >= "2024-08-01")
    .select(
        "src.id",
        "src.session_id",
        "src.event_time_hour",
        "src.is_new_session",
        "src.island_id",
        "src.last4_sum_island_id",
        "src.last4_max_island_id",
        "src.is_new_session_2",
        F.col("p.device"),
        F.col("p.browser"),
        F.col("p.country"),
    )
)

stats = (
    with_dims
    .cube("country")
    .agg(
        F.round(F.sum("is_new_session_2") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("is_new_session_2_per_firefox"),
        F.percentile_approx("last4_max_island_id", [0.25, 0.5, 0.75]).alias("last4_max_island_id_quartiles"),
        F.sum(F.when(F.col("country") == "TH", F.col("island_id")).otherwise(0)).alias("th_island_id"),
    )
)

result = (
    stats
    .select(
        "country",
        "is_new_session_2_per_firefox",
        "last4_max_island_id_quartiles",
        F.when(F.col("country").rlike("^(UK|TH)$"), "OTHER_ZONE").when(F.col("country").rlike("^(US|SG)$"), "EXTENDED").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("is_new_session_2_per_firefox").desc(), "country")
)
