from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

deduped = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "account_type", "opened_date")
)

result = (
    deduped
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "PRIORITY").otherwise("EXTENDED").alias("account_type_zone"),
        F.col("min_subscription_monthly_fee"),
    )
)
