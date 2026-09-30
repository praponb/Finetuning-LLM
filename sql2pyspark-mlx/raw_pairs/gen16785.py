from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

enriched = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.account_type", "s.status", "s.credit_limit", F.col("c.txn_type"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type") == "FEE"), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.txn_type",
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    with_last
    .select(
        "id",
        "txn_type",
        "transaction_amount",
        F.when(F.col("txn_type").rlike("^(FEE|CREDIT)$"), "GROUP_A").otherwise("GROUP_B").alias("txn_type_zone"),
    )
)
