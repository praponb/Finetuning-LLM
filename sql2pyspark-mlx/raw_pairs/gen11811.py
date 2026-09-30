from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("page", "session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("page")

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-10-11"))) & F.col("page").isNotNull())
    .withColumn("event_type_upper", F.upper("event_type"))
)

result = (
    valid
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.count("duration_sec").over(w_running).alias("last4_count_duration_sec"),
        F.max("duration_sec").over(w_running).alias("last4_max_duration_sec"),
        F.sum("duration_sec").over(w_running).alias("last4_sum_duration_sec"),
        F.min("duration_sec").over(w_running).alias("last4_min_duration_sec"),
        F.max("duration_sec").over(w_grp).alias("page_max_duration_sec"),
    )
)
