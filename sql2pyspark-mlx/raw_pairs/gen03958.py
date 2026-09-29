from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("months_since_event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("MONTH", "event_time"), "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("months_since_event_time").desc_nulls_first(), "id")
w5 = Window.orderBy("prev_months_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type")
w7 = Window.partitionBy(F.date_trunc("YEAR", "event_time"), "event_type").orderBy("months_since_event_time", "id")
w8 = Window.partitionBy("session_id").orderBy("months_since_event_time", "id")
w9 = Window.partitionBy("page").orderBy("duration_sec_count_7d", "id")

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-06-11"))) & F.col("page").isNotNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "event_time") / 30).alias("months_since_event_time"),
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

with_windows = (
    valid
    .filter(F.col("months_since_event_time") <= 51)
    .select(
        "*",
        F.lag("months_since_event_time", 1, 0).over(w).alias("prev_months_since_event_time"),
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_7d"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

with_windows_web_events = (
    deduped
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_type_title",
        "page_clean",
        "last_known_duration_sec",
        "row_num",
        F.first("months_since_event_time", ignorenulls=True).over(w5).alias("first_known_months_since_event_time"),
        F.count("last_known_duration_sec").over(w6).alias("event_type_count_last_known_duration_sec"),
        F.lag("prev_months_since_event_time", 2, 0).over(w7).alias("prev_prev_months_since_event_time"),
        F.lag("months_since_event_time", 1, 0).over(w8).alias("prev_months_since_event_time_2"),
        F.lead("months_since_event_time", 1).over(w9).alias("next_months_since_event_time"),
    )
)

cleaned = (
    with_windows_web_events
    .filter(F.col("first_known_months_since_event_time").between(43, 79))
    .withColumn("first_known_months_since_event_time_level", F.when(F.col("first_known_months_since_event_time") >= 62, "High").when(F.col("first_known_months_since_event_time") >= 10, "Medium").otherwise("Low"))
    .withColumn("page_clean_2", F.coalesce("page", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .alias("src")
    .join(df_sessions.alias("dim"), F.col("src.session_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.device").alias("device"),
        F.col("dim.country").alias("country"),
        F.col("src.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("src.event_type_count_last_known_duration_sec")).alias("avg_event_type_count_last_known_duration_sec"),
    )
)
