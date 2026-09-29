from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("end_date", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

scored = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .withColumn("prev_monthly_fee", F.lag("monthly_fee").over(w_recent))
    .withColumn("next_monthly_fee", F.lead("monthly_fee").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

result = (
    scored
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "prev_monthly_fee",
        "next_monthly_fee",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy("start_date")
)
