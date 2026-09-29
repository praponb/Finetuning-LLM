from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("started_at"), "browser").orderBy("started_at", "id")
w_part = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at", "id")
w5 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy("started_at", "id")
w7 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Chrome"]))
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("browser", ignorenulls=True).over(w_part).alias("last_known_browser"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser_2"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w4).isNull() | (F.col("started_at") > F.lag("started_at").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "last_known_browser",
        "last_known_browser_2",
        F.sum("is_new_burst").over(w5).alias("session_no"),
    )
)

with_gaps = (
    sessionized
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w6).isNull() | (F.col("started_at") > F.lag("started_at").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_sessions = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w7))

prev_events = sessionized_sessions.withColumn("prev_started_at", F.lag("started_at").over(w4))

gaps_2 = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps_2.withColumn("session_id_2", F.sum("is_new_session").over(w5))

by_type = (
    sessions_sessions
    .groupBy("device")
    .pivot("country", ["TH", "SG", "DE"])
    .agg(F.max("new_group_flag"))
)

result = (
    by_type
    .select(
        "device",
        "DE",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "PRIORITY").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy("DE")
)
