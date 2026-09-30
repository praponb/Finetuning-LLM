from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        "session_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .select("id", "page", "duration_sec", "session_id")
    .orderBy("page", F.col("duration_sec").desc())
)
