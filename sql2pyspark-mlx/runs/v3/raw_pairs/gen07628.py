from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.ntile(10).over(w).alias("duration_sec_ntile"),
    )
)
