from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.first("status").over(w2).alias("first_status"),
    )
)
