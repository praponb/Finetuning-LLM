from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-06-10")))
    .withColumn("account_type_status_key", F.concat_ws("/", "account_type", "status"))
    .withColumn("status_filled", F.coalesce("status", F.lit("UNKNOWN")))
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type_status_key",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.is_flagged")), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

result = (
    with_latest
    .select(
        "id",
        "customer_id",
        "account_type_status_key",
        "txn_type",
        F.when(F.col("txn_type").rlike("^(FEE|REFUND)$"), "EXTENDED").otherwise("PRIORITY").alias("txn_type_zone"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
