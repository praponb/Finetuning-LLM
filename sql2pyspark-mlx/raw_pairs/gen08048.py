from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time")

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["add_to_cart", "search", "purchase"]))
    .select("id", "event_type", "event_time", F.rank().over(w).alias("event_time_rank"))
)
