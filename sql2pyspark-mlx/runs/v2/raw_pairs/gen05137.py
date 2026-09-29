from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select("id", "country", "started_at", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "country", "started_at")
    .orderBy("country")
)
