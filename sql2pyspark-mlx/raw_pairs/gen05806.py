from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time_weekofyear").desc_nulls_last(), "id")
w2 = Window.partitionBy("session_id", "page").orderBy(F.col("event_time").desc(), "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.weekofyear("event_time").alias("event_time_weekofyear"),
        (F.datediff(F.current_date(), "event_time") / 365).alias("years_since_event_time"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "event_time_weekofyear",
        "years_since_event_time",
    )
)

filtered = (
    ranked
    .filter((F.col("event_time") >= F.lit("2023-06-04 00:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "event_time_weekofyear",
        "years_since_event_time",
        F.when(F.col("years_since_event_time") >= 63, "High").when(F.col("years_since_event_time") >= 25, "Medium").otherwise("Low").alias("years_since_event_time_level"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .select(
        "id",
        "session_id",
        "page",
        "event_time_weekofyear",
        "years_since_event_time",
        "years_since_event_time_level",
    )
    .orderBy(F.col("event_time_weekofyear").desc_nulls_last(), F.col("id").desc())
)
