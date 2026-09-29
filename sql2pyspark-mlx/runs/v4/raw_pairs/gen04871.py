from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        F.lag("started_at", 1).over(w).alias("prev_started_at"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy(F.col("row_num").desc())
)
