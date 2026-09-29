from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date")
w2 = Window.partitionBy("status").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        F.rank().over(w).alias("rnk"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy("id")
)
