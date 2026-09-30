from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("started_at") >= "2024-10-01")
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.first("country").over(w).alias("first_country"),
    )
)
