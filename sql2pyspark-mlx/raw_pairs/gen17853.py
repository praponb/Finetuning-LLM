from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/search", "/product"]))
    .select("id", "event_type", F.count("*").over(w).alias("event_type_count"))
)
