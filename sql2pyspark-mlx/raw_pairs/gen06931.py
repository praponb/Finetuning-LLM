from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("txn_type").orderBy(F.col("opened_date").desc_nulls_first(), "id")

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-17").cast("date"))
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.col("balance").cast("int").alias("balance_int"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "opened_date", "balance_int")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.opened_date",
        "s.balance_int",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant") == "Starbucks"), "left_anti")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.avg(F.when(F.col("txn_type") == "CREDIT", F.col("transaction_amount"))).alias("credit_avg_transaction_amount"),
    )
    .orderBy("credit_count")
)
