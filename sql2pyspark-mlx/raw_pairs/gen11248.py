from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.lag("started_at", 1).over(w).alias("prev_started_at"),
    )
)
