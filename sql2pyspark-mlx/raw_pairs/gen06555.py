from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "session_id", "page", "event_time", "duration_sec")
)

base = (
    ranked
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-05-20")))
    .withColumn("page_filled", F.coalesce("page", F.lit("unassigned")))
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.sum("new_group_flag").over(w3).alias("burst_id"),
    )
)

cleaned = (
    sessionized
    .filter(F.col("event_time") < "2024-10-01")
    .select(
        "id",
        "session_id",
        "event_time",
        "burst_id",
        F.upper("page").alias("page_upper"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

cleaned_web_events = (
    cleaned
    .filter(F.col("session_id").isin([17, 23]))
    .select(
        "*",
        F.when(F.col("burst_id") >= 8, "High").when(F.col("burst_id") >= 2, "Medium").otherwise("Low").alias("burst_id_level"),
        F.when(F.col("burst_id") >= 7, "High").when(F.col("burst_id") >= 2, "Medium").otherwise("Low").alias("burst_id_tier"),
    )
)

stats = (
    cleaned_web_events
    .rollup("session_id", "burst_id")
    .agg(
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1000)
)

result = (
    stats
    .select("distinct_session_ids")
    .orderBy(F.col("distinct_session_ids").asc_nulls_last())
)
