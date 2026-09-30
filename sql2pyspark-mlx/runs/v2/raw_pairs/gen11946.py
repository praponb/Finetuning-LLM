from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([6, 19]))
    .select(
        "id",
        "session_id",
        "duration_sec",
        "page",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "session_id", "duration_sec", "page")
)
