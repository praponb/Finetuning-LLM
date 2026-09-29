from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "device").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-01-01")
    .select(
        "id",
        "country",
        "device",
        "started_at",
        F.lead("started_at", 1).over(w).alias("next_started_at"),
    )
)
