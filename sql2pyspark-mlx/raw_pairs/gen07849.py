from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_first())
w5 = Window.partitionBy("event_type", "page").orderBy("session_start_flag")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/cart", "/product"]))
    .withColumn("page_event_type_key", F.concat_ws("-", "page", "event_type"))
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w3))

deduped_web_events = (
    sessionized
    .withColumn("rank_in_group_2", F.rank().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
)

latest = (
    deduped_web_events
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "page_event_type_key",
        "rank_in_group",
        "session_start_flag",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("event_type").desc())
)
