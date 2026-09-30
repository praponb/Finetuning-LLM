from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("duration_sec") != 303)
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.rank().over(w).alias("duration_sec_rank"),
    )
)
