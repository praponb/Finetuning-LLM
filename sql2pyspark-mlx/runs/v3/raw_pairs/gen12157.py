from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
