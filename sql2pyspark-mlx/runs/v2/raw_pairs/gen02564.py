from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date")

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED"]))
    .select(
        "id",
        "status",
        "end_date",
        "start_date",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "status", "end_date", "start_date")
    .orderBy("end_date")
)
