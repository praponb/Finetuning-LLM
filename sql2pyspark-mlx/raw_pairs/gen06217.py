from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("cumulative_duration_sec_2_num_whole", "id")

with_windows = (
    df_web_events
    .select(
        "id",
        "event_type",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec_2"),
    )
)

base = (
    with_windows
    .filter(F.col("event_type").isin(["page_view", "click"]))
    .select(
        "*",
        F.col("cumulative_duration_sec_2").cast("double").alias("cumulative_duration_sec_2_num"),
    )
)

base_web_events = (
    base
    .filter(F.col("cumulative_duration_sec_2_num") <= 67)
    .select(
        "id",
        "event_type",
        "cumulative_duration_sec_2_num",
        F.floor("cumulative_duration_sec_2_num").alias("cumulative_duration_sec_2_num_whole"),
    )
)

deduped = (
    base_web_events
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "event_type",
        "cumulative_duration_sec_2_num_whole",
        "row_num",
        F.when(F.col("event_type").rlike("^(click|page_view)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("event_type_zone"),
    )
)
