from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "*",
        F.lead("duration_sec").over(w_ordered).alias("next_duration_sec"),
        F.lag("duration_sec").over(w_ordered).alias("prev_duration_sec"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)
