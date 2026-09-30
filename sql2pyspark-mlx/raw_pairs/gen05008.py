from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

cleaned = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "click", "purchase"]))
    .select(
        "*",
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
        F.round("duration_sec", 2).alias("duration_sec_rounded_2"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
