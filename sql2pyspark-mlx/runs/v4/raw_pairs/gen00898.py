from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store", "partner"]))
    .select("id", "channel", F.count("*").over(w).alias("channel_count"))
    .orderBy("id")
)
