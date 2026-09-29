from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")

result = df_web_events.select("id", "event_time", F.row_number().over(w).alias("row_num"))
