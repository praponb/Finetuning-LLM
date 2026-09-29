from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("adjusted_duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("rank_in_group").desc())

valid = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.round((F.col("duration_sec") * 1.1), 2).alias("adjusted_duration_sec"),
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_web_events = (
    latest
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked = (
    latest_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "event_type", "duration_sec", "adjusted_duration_sec", "page_clean")
)

result = (
    ranked
    .select(
        "id",
        "event_type",
        "duration_sec",
        "adjusted_duration_sec",
        "page_clean",
        F.when(F.col("event_type").rlike("^(add_to_cart|search)$"), "EXTENDED").otherwise("GROUP_B").alias("event_type_zone"),
    )
    .orderBy("event_type")
)
