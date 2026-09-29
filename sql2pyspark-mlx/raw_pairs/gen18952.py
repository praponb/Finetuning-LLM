from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.partitionBy(F.year("opened_date")).orderBy("balance", "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("seq_no").desc_nulls_first(), "id")
w4 = Window.orderBy("seq_no_2", "id")
w5 = Window.partitionBy(F.month("opened_date")).orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("seq_no").desc(), "id")
w7 = Window.partitionBy(F.date_trunc("month", "opened_date"), "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

cleaned = (
    df_accounts
    .filter((F.col("status").eqNullSafe(F.lit("CLOSED"))) & (F.col("balance") <= 58000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.trim("account_type").alias("clean_account_type"),
        F.coalesce("status", F.lit("NONE")).alias("status_filled"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["Lazada", "Shell", "Amazon"])), "left_semi")
)

metrics = (
    kept
    .filter(F.col("opened_date") < "2024-04-01")
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.is_flagged")), "left_anti")
)

latest = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .withColumn("prev_balance", F.lag("balance", 2).over(w4))
    .withColumn("running_seq_no", F.sum("seq_no").over(w5))
    .withColumn("seq_no_bucket", F.ntile(5).over(w6))
    .withColumn("last_known_seq_no", F.last("seq_no", ignorenulls=True).over(w7))
)

deduped = (
    with_windows
    .withColumn("rank_in_group_2", F.row_number().over(w8))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    deduped
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "clean_account_type",
        "status_filled",
        "seq_no_2",
        F.col("max_subscription_monthly_fee"),
    )
)
