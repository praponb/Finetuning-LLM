from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select("id", "device", "started_at", "country", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "device", "started_at", "country")
)
