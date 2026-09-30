from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-5, -1)

base = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-08-21").cast("date"))
    .select("*", F.initcap(F.trim("event_type")).alias("event_type_title"))
)

result = (
    base
    .filter(~F.col("event_type").isin(["add_to_cart", "click"]))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "event_type_title",
        F.count("duration_sec").over(w).alias("duration_sec_count_90d"),
        F.lead("duration_sec", 1, 0).over(w2).alias("next_duration_sec"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_5_prior"),
    )
)
