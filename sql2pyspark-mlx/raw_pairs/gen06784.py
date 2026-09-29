from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("page", "event_type").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id", "event_type").orderBy(F.col("session_id_2").desc(), "id")

ranked = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

flagged = (
    deduped
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.sum("new_group_flag").over(w4).alias("session_id_2"),
    )
)

latest = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = latest.select("id", "event_time", "session_id_2", "dedup_rank")
