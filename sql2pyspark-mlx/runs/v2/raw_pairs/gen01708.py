from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.percent_rank().over(w).alias("duration_sec_pct_rank"),
    )
)
