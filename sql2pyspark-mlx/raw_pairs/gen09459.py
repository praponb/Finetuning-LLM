from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        F.dense_rank().over(w).alias("start_date_dense_rank"),
    )
    .orderBy("customer_id")
)
