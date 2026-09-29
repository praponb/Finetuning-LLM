from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "started_at",
        F.lead("started_at", 1).over(w).alias("next_started_at"),
        F.rank().over(w2).alias("started_at_rank"),
    )
    .orderBy(F.col("next_started_at").desc())
)
