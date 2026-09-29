from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.avg("monthly_fee").over(w).alias("plan_avg_monthly_fee"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
    )
)
