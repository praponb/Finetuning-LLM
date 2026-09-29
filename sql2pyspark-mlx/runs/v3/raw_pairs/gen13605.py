from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "purchase"]))
    .select("id", "session_id", "event_time", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("rnk").desc())
)
