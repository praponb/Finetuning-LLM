from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "started_at"), "customer_id").orderBy("started_at")

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "customer_id",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
    )
)
