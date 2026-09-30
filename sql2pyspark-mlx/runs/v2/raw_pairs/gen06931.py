from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee")

result = (
    df_subscriptions
    .select("id", "monthly_fee", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy("dense_rnk", F.col("monthly_fee").desc())
)
