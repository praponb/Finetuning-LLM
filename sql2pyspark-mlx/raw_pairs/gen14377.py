from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

metrics = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w_grp).alias("last3_avg_balance"),
        F.min("balance").over(w_grp).alias("last3_min_balance"),
        F.sum("balance").over(w_grp).alias("last3_sum_balance"),
        F.first("status", ignorenulls=True).over(w_recent).alias("first_known_status"),
    )
)

result = (
    metrics
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "last3_avg_balance",
        "last3_min_balance",
        "last3_sum_balance",
        F.col("sum_subscription_monthly_fee"),
    )
)
