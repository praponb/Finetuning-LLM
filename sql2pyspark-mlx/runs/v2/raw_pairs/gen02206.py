from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
    )
)
