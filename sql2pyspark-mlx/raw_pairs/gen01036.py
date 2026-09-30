from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec_adjusted").desc(), "id")
w2 = Window.partitionBy("session_id", "event_type").orderBy(F.col("rank_in_group").desc(), "id")

base = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-07-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.round((F.col("duration_sec") * 100), 2).alias("duration_sec_adjusted"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.device"), F.col("s.country"), F.col("s.browser"))
)

result = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec_adjusted",
        "rank_in_group",
        "row_num",
        "device",
        "country",
    )
)
