from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at")

result = (
    df_sessions
    .select(
        "id",
        "country",
        "started_at",
        "device",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "country", "started_at", "device")
    .orderBy("device")
)
