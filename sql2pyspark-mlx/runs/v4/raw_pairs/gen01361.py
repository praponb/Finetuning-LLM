from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

ranked = (
    df_accounts
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "latest_rank",
        F.when(F.col("status").rlike("^(FROZEN|OPEN)$"), "EXTENDED").when(F.col("status").rlike("^(CLOSED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last(), F.col("id").desc())
)
