from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("id", "page", "duration_sec", F.rank().over(w).alias("rnk"))
)
