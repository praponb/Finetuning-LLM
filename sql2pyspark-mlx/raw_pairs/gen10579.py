from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w_running = Window.partitionBy("plan", "status").orderBy("start_date", "id")
w4 = Window.partitionBy(F.month("start_date"), "plan").orderBy("end_date", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

enriched = (
    df_subscriptions
    .filter(F.year("start_date") >= 2024)
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.rank().over(w2).alias("monthly_fee_rank"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "first_known_monthly_fee",
        "monthly_fee_rank",
        F.col("sum_subscription_monthly_fee"),
    )
)
