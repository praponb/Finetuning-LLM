from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").desc())

result = df_web_events.select("id", "event_time", F.rank().over(w).alias("event_time_rank"))
