from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        F.lead("started_at", 2).over(w).alias("next_started_at"),
    )
    .orderBy("started_at")
)
