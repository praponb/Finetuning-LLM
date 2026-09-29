from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("started_at"), "device").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "device",
        F.lag("started_at", 2).over(w).alias("prev_started_at"),
    )
)
