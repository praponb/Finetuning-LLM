from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "device",
        F.lag("started_at", 1).over(w).alias("prev_started_at"),
        F.percent_rank().over(w2).alias("started_at_pct_rank"),
    )
    .orderBy(F.col("started_at").desc(), "started_at_pct_rank")
)
