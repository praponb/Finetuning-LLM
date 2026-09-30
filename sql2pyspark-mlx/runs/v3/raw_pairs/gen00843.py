from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "browser",
        "started_at",
        F.lag("started_at", 1).over(w).alias("prev_started_at"),
    )
)
