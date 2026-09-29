from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("page_length").desc_nulls_last(), "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.length("page").alias("page_length"),
    )
)

result = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "page")
)
