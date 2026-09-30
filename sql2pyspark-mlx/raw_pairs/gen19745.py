from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("session_id").isin([15, 24, 33]))
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.rank().over(w).alias("duration_sec_rank"),
    )
    .orderBy(F.col("duration_sec_rank").desc())
)
