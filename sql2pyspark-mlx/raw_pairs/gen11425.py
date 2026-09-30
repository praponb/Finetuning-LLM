from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("duration_sec", "id").rowsBetween(-5, -1)
w2 = Window.orderBy(F.col("is_search").desc_nulls_last())
w3 = Window.partitionBy("page", "session_id").orderBy(F.col("is_search").desc(), "id")
w_recent = Window.orderBy(F.col("is_search").desc())
w5 = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id").rowsBetween(-2, -1)

filtered = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-03-01")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("page") == "/search", 1).otherwise(0).alias("is_search"),
        F.trim("event_type").alias("clean_event_type"),
    )
)

result = (
    filtered
    .select(
        "id",
        "event_time",
        "is_search",
        F.avg("is_search").over(w).alias("is_search_avg_5_prior"),
        F.percent_rank().over(w2).alias("is_search_percent_rank"),
        F.ntile(10).over(w3).alias("is_search_bucket"),
        F.percent_rank().over(w_recent).alias("is_search_percent_rank_2"),
        F.avg("duration_sec").over(w5).alias("duration_sec_avg_2_prior"),
    )
)
