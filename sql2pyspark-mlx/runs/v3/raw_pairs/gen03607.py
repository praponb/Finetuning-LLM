from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "plan",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "customer_id", "monthly_fee", "plan")
    .orderBy(F.col("plan").desc())
)
