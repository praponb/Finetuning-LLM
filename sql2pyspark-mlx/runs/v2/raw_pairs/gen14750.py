from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select("id", "started_at", F.rank().over(w).alias("started_at_rank"))
    .orderBy("started_at")
)
