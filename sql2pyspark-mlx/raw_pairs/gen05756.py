from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "plan",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "customer_id", "monthly_fee", "plan")
    .orderBy(F.col("id").desc(), "customer_id")
)
