from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

ranked = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type").rlike("^(page_view|click)$"), "PRIORITY").otherwise("CORE").alias("event_type_zone"),
    )
    .orderBy(F.col("event_type_zone").asc_nulls_first())
)
