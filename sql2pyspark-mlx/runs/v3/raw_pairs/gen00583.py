from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "plan",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.min("monthly_fee").over(w2).alias("plan_min_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
