from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "customer_id",
        F.lag("started_at", 1).over(w).alias("prev_started_at"),
        F.row_number().over(w2).alias("rn"),
    )
)
