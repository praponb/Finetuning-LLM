from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w5 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

scored = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("account_type_stddev_samp_balance", F.stddev_samp("balance").over(w))
    .withColumn("prev_balance", F.lag("balance", 2, 0).over(w2))
    .withColumn("next_balance", F.lead("balance", 1).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("cumulative_balance", F.sum("balance").over(w5))
)

result = (
    scored
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "account_type_stddev_samp_balance",
        "prev_balance",
        "cumulative_balance",
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
