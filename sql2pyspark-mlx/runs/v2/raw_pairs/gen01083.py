from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at")
w2 = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.rank().over(w).alias("started_at_rank"),
        F.lead("started_at", 2).over(w2).alias("next_started_at"),
    )
)
