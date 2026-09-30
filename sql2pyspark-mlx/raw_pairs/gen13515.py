from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "customer_id").orderBy("end_date", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

with_windows = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.lag("monthly_fee").over(w_ordered).alias("prev_monthly_fee"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "prev_monthly_fee",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
)
