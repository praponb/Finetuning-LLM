from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

latest = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("plan").rlike("^(standard|basic)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(premium|enterprise)$"), "GROUP_B").otherwise("CORE").alias("plan_zone"),
        F.col("sum_account_balance"),
    )
)
