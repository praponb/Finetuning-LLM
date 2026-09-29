from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("status")

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-06-05")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("status").isin(["CLOSED", "OPEN"]), "Priority").otherwise("Other").alias("status_group"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

valid = (
    eligible
    .filter(F.year("opened_date") >= 2024)
    .select(
        "*",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.round("balance", 0).alias("balance_rounded"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.status_group",
        "s.resolved_status",
        "s.opened_date_label",
        F.col("c.txn_type"),
    )
)

scored = (
    latest_match
    .filter(F.col("balance") > 58000)
    .select(
        "id",
        "status",
        "status_group",
        "opened_date_label",
        F.lead("balance").over(w2).alias("next_balance"),
        F.row_number().over(w2).alias("event_seq"),
        F.min("balance").over(w3).alias("status_min_balance"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.hour(F.col("x.txn_time")).between(8, 11)), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "status",
        "opened_date_label",
        "next_balance",
        "event_seq",
        "status_min_balance",
    )
    .orderBy(F.col("next_balance").asc_nulls_first())
)
