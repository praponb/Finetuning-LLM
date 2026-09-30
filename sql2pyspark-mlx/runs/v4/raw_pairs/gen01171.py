from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last(), "id")
w_running = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("status", "account_type")
w6 = Window.partitionBy("customer_id").orderBy("balance_avg_2_prior", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

valid = (
    df_accounts
    .filter(F.col("balance").between(41000, 76000))
    .select("*", F.substring("account_type", 1, 4).alias("account_type_prefix"))
)

scored = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w2))
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.avg("balance").over(w_running).alias("balance_avg_2_prior"),
        F.sum("seq_no").over(w5).alias("status_sum_seq_no"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group_2", F.row_number().over(w6))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

result = (
    latest
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "opened_date",
        "seq_no",
        "first_known_status",
        "status_sum_seq_no",
        F.col("sum_account_balance"),
    )
    .orderBy(F.col("id").desc())
)
