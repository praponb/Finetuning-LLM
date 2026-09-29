from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "account_type").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isin([2, 5, 15, 28]))
    .withColumn("opened_date_plus_42d", F.date_add("opened_date", 42))
)

enriched = (
    valid
    .filter(F.col("opened_date_plus_42d") > "2024-08-14")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.last("status", ignorenulls=True).over(w2).alias("last_known_status"),
        F.avg("balance").over(w_ordered).alias("balance_avg_2"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.name"), F.col("d.age"))
)

latest_match = (
    joined
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance_dense_rank",
        "s.last_known_status",
        "s.balance_avg_2",
        "s.name",
        "s.age",
        F.col("c.amount"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (~F.col("ch.is_flagged")), "left_anti")
)

kept_keys = (
    eligible
    .select("customer_id")
    .subtract(
        df_accounts
        .filter(F.col("balance") < 40000)
        .select("customer_id")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance_dense_rank",
        "last_known_status",
        "balance_avg_2",
        "name",
        "amount",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("id").desc(), "balance_dense_rank")
)
