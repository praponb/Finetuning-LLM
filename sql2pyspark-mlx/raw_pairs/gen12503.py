from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "session_start_flag",
        "session_no",
        F.when(F.col("event_type").rlike("^(click|page_view)$"), "OTHER_ZONE").when(F.col("event_type").rlike("^(add_to_cart|purchase)$"), "CORE").otherwise("GROUP_A").alias("event_type_zone"),
    )
    .orderBy("session_id")
)
