from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")

result = df_web_events.select("id", "page", F.count("*").over(w).alias("page_count"))
