from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([12, 20, 22, 25]))
    .select(
        "id",
        "customer_id",
        "start_date",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
