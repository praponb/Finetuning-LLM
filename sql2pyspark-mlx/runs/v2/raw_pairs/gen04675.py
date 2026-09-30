from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        F.percent_rank().over(w).alias("started_at_pct_rank"),
    )
    .orderBy("id")
)
