from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w2 = Window.orderBy("start_date")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(160, 220))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("rnk").desc())
)
