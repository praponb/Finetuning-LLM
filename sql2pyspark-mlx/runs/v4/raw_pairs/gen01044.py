from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/product"]))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.count("*").over(w).alias("event_type_count"),
        F.rank().over(w2).alias("rnk"),
    )
)
