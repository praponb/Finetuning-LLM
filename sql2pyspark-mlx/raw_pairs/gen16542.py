from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec_whole").desc(), "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("page").isin(["/checkout", "/home", "/search"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
