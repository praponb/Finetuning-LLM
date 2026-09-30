from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "customer_id", "start_date", "end_date")
)
