from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select("id", "device", "started_at", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "device", "started_at")
    .orderBy("device", "started_at")
)
