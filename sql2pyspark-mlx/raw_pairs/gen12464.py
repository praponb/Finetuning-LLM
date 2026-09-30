from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at")

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2025-05-01")
    .select(
        "id",
        "device",
        "started_at",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
    )
    .orderBy("device")
)
