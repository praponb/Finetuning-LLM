from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at")

result = (
    df_sessions
    .select("id", "started_at", F.percent_rank().over(w).alias("started_at_pct_rank"))
    .orderBy("started_at")
)
