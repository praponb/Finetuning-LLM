from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date_period", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("status", "account_type").orderBy("opened_date_period", "id")
w_ordered = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy("account_type").orderBy(F.col("event_seq").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("customer_id").orderBy("balance_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("balance_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("first_known_balance_avg_6_prior", "id").rowsBetween(-2, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-05-06").cast("date")) & F.col("status").isNotNull())
    .withColumn("opened_date_period", F.date_trunc("month", "opened_date"))
)

metrics = (
    filtered
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

eligible = (
    metrics
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id") == 11), "left_anti")
)

enriched = (
    eligible
    .select(
        "*",
        F.row_number().over(w_ordered).alias("event_seq_2"),
        F.ntile(4).over(w4).alias("event_seq_bucket"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date_period")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

scored = (
    with_last
    .filter(F.col("transaction_txn_type").isin(["FEE", "REFUND"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date_period",
        "event_seq",
        "event_seq_bucket",
        F.count("balance").over(w_recent).alias("running_count_balance"),
        F.sum("balance").over(w_recent).alias("running_sum_balance"),
        F.min("balance").over(w_recent).alias("running_min_balance"),
        F.first("balance_avg_6_prior", ignorenulls=True).over(w7).alias("first_known_balance_avg_6_prior"),
    )
)

with_windows = (
    scored
    .withColumn("last3_min_event_seq_bucket", F.min("event_seq_bucket").over(w8))
    .withColumn("last3_max_event_seq_bucket", F.max("event_seq_bucket").over(w8))
    .withColumn("last3_count_event_seq_bucket", F.count("event_seq_bucket").over(w8))
    .withColumn("last3_sum_event_seq_bucket", F.sum("event_seq_bucket").over(w8))
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date_period",
        "event_seq",
        "running_count_balance",
        "running_sum_balance",
        "running_min_balance",
        "first_known_balance_avg_6_prior",
        F.when(F.col("account_type").rlike("^(SAVINGS|CREDIT)$"), "EXTENDED").when(F.col("account_type").rlike("^(CHECKING|LOAN)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("account_type_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
