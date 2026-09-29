from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_type",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
)
