from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "customer_id",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w2).alias("customer_id_avg_monthly_fee"),
    )
)
