from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at")

result = (
    df_sessions
    .select("id", "browser", "started_at", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .select("id", "browser", "started_at")
    .orderBy(F.col("id").desc())
)
