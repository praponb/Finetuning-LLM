from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_first(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

base = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["SAVINGS", "CHECKING"]), "Priority").otherwise("Other"))
    .withColumn("status_clean", F.coalesce("status", F.lit("N/A")))
)

latest = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_accounts = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

base_accounts = (
    latest_accounts
    .filter((F.col("opened_date") >= F.lit("2024-03-09").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.least("rn", "rank_in_group").alias("least_rn_rank_in_group"))
)

pivoted = (
    base_accounts
    .groupBy("customer_id", "status")
    .pivot("account_type", ["CHECKING", "LOAN"])
    .agg(F.avg("least_rn_rank_in_group"))
)

result = (
    pivoted
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "status", "CHECKING", F.col("sum_subscription_monthly_fee"))
    .orderBy(F.col("CHECKING").desc())
)
