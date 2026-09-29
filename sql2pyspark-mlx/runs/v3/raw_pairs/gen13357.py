from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "start_date",
        F.rank().over(w).alias("rnk"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
    .orderBy(F.col("running_monthly_fee").desc())
)
