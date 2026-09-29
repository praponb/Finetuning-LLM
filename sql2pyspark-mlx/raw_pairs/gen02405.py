from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.account_type", F.col("c.txn_type").alias("transaction_txn_type"))
)

result = (
    with_last
    .select("id", "account_type", "transaction_txn_type")
    .orderBy(F.col("transaction_txn_type").desc_nulls_first())
)
