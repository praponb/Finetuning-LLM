from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "device",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "browser", "started_at", "device")
    .orderBy(F.col("id").desc(), "device")
)
