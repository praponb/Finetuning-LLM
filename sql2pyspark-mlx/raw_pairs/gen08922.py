from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee").asc_nulls_last())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

metrics = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_6"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
    )
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "start_date", "end_date", F.col("min_account_balance"))
)
