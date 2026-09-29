from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

with_windows = (
    df_subscriptions
    .withColumn("next_monthly_fee", F.lead("monthly_fee").over(w))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "next_monthly_fee",
        "event_seq",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy("sum_subscription_monthly_fee")
)
