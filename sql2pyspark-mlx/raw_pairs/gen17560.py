from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-09-26").cast("date"))
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.city") == "New York")
    .select(
        "e.id",
        "e.customer_id",
        "e.balance",
        "e.opened_date",
        "e.status",
        F.col("b.city"),
        F.col("b.email"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "balance", "opened_date", "status", "city", "email")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["7-Eleven", "Shell"])), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "city",
        "email",
        "transaction_txn_time",
        "txn_type",
        F.when(F.col("txn_type").rlike("^(CREDIT|DEBIT)$"), "PRIORITY").when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "CORE").otherwise("GROUP_A").alias("txn_type_zone"),
    )
    .orderBy(F.col("transaction_txn_time").desc())
)
