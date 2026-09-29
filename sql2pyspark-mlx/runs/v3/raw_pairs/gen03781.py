from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("start_date")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "start_date",
        F.row_number().over(w).alias("row_num"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
