from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")

result = (
    df_sessions
    .select("id", "country", "started_at", F.rank().over(w).alias("started_at_rank"))
    .orderBy("started_at_rank")
)
