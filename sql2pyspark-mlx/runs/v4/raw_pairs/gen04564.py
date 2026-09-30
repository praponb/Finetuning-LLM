from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 110)
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "start_date",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
)
