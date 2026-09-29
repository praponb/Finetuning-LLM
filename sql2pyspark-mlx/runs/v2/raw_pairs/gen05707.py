from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/cart"]))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        (F.col("duration_sec") - F.lag("duration_sec", 1, 0).over(w)).alias("duration_sec_change"),
    )
)
