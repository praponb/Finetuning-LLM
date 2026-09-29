from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page", "session_id").orderBy("new_group_flag", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("page", "event_type").orderBy("last7_max_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.to_date("event_time")).orderBy("last7_avg_duration_sec", "id")
w6 = Window.partitionBy("page").orderBy("last7_count_duration_sec", "id")
w7 = Window.partitionBy(F.year("event_time"), "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("event_type").orderBy("last7_count_duration_sec", "id")

valid = (
    df_web_events
    .filter(F.col("duration_sec") <= 214)
    .withColumn("page_group", F.when(F.col("page").isin(["/product", "/checkout"]), "Tier 1").otherwise("Other"))
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

with_windows = (
    sessions_web_events
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "page_group",
        "new_group_flag",
        "burst_id",
        F.avg("duration_sec").over(w3).alias("last7_avg_duration_sec"),
        F.count("duration_sec").over(w3).alias("last7_count_duration_sec"),
        F.max("duration_sec").over(w3).alias("last7_max_duration_sec"),
    )
)

scored = (
    with_windows
    .filter(F.col("last7_avg_duration_sec") < 480)
    .select(
        "id",
        "event_type",
        "event_time",
        "last7_avg_duration_sec",
        "last7_max_duration_sec",
        F.last("duration_sec", ignorenulls=True).over(w4).alias("last_known_duration_sec"),
        F.row_number().over(w5).alias("event_seq"),
        F.lag("last7_avg_duration_sec", 1).over(w6).alias("prev_last7_avg_duration_sec"),
        F.count("burst_id").over(w7).alias("burst_id_count_90d"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_type",
        "event_time",
        "last7_avg_duration_sec",
        "last7_max_duration_sec",
        "last_known_duration_sec",
        "burst_id_count_90d",
        "row_seq",
        F.when(F.col("event_type").rlike("^(page_view|purchase)$"), "GROUP_B").otherwise("GROUP_A").alias("event_type_zone"),
    )
    .orderBy("event_type_zone")
)
