from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date")
w2 = Window.partitionBy("status").orderBy("end_date")

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2023-09-02")
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        "end_date",
        F.rank().over(w).alias("rnk"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy("end_date")
)
