from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2024-05-06 21:00:00").cast("timestamp")) & (F.col("event_time") >= "2023-12-01"))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.length("page").alias("page_length"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

valid = (
    sessionized
    .filter(F.col("event_time") >= F.lit("2023-04-23").cast("date"))
    .select(
        "id",
        "event_type",
        "event_time",
        "page_length",
        F.when(F.col("duration_sec") >= 362, "High").when(F.col("duration_sec") >= 117, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.col("new_group_flag").cast("double").alias("new_group_flag_num"),
    )
)

flagged_2 = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged_2.withColumn("session_no", F.sum("is_new_burst").over(w2))

latest = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy("event_type", F.date_trunc("quarter", "event_time").alias("quarter_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("event_type") == "page_view").alias("page_view_events"),
    )
)
