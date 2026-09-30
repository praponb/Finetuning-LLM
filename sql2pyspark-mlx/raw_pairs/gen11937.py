from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

enriched = (
    df_accounts
    .select(
        "*",
        F.sum("balance").over(w).alias("running_balance"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
    )
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "running_balance",
        "first_known_status",
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
