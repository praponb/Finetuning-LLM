from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("txn_type")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy(F.month("opened_date"), "status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"))
w6 = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.floor("balance").alias("balance_whole"),
        F.length("account_type").alias("account_type_length"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.balance_whole",
        "s.account_type_length",
        F.col("c.txn_time"),
        F.col("c.txn_type"),
    )
)

kept_keys = (
    latest_match
    .select("id", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "FROZEN"]))
        .select("id", "customer_id")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .filter(F.hour("txn_time").between(6, 12))
    .select(
        "opened_date",
        "status",
        "balance_whole",
        "txn_time",
        F.sum("balance_whole").over(w_ordered).alias("txn_type_sum_balance_whole"),
        F.sum("balance").over(w3).alias("customer_id_sum_balance"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.sum("txn_type_sum_balance_whole").over(w4).alias("txn_type_sum_balance_whole_sum_30d"),
        F.avg("customer_id_sum_balance").over(w5).alias("opened_date_period_avg_customer_id_sum_balance"),
    )
)

metrics = (
    enriched
    .withColumn("balance_whole_count_30d", F.count("balance_whole").over(w6))
    .withColumn("txn_type_sum_balance_whole_sum_30d_avg_90d", F.avg("txn_type_sum_balance_whole_sum_30d").over(w7))
    .withColumn("customer_id_sum_balance_avg_30d", F.avg("customer_id_sum_balance").over(w8))
)

kept_keys_2 = (
    metrics
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "LOAN")
        .select("status")
    )
)

reconciled_accounts = (
    metrics
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled_accounts
    .select(
        "balance_whole",
        "txn_time",
        "customer_id_sum_balance",
        "txn_type_sum_balance_whole_sum_30d",
        "opened_date_period_avg_customer_id_sum_balance",
        "txn_type_sum_balance_whole_sum_30d_avg_90d",
    )
    .orderBy(
        F.col("opened_date_period_avg_customer_id_sum_balance").desc_nulls_last(),
        "txn_type_sum_balance_whole_sum_30d_avg_90d",
    )
)
