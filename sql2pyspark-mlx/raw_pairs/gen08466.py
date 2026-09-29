from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.partitionBy("page", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

enriched = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.min("duration_sec").over(w).alias("event_type_min_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
        F.sum("duration_sec").over(w3).alias("duration_sec_sum_30d"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_type",
        "duration_sec",
        "event_type_min_duration_sec",
        "duration_sec_cume_dist",
        F.when(F.col("event_type").rlike("^(page_view|click)$"), "PRIORITY").when(F.col("event_type").rlike("^(add_to_cart|purchase)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("event_type_zone"),
    )
    .orderBy("event_type", "event_type_min_duration_sec")
)
