from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "purchase"]))
    .select("id", "duration_sec", F.row_number().over(w).alias("rn"))
)
