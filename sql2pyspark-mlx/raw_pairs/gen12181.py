from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "event_type", "event_time", "duration_sec")
)
