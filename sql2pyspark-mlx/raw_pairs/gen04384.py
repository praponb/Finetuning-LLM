from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter((F.col("status").eqNullSafe(F.lit("OPEN"))) & (F.col("opened_date") >= "2023-04-27"))
    .select(
        "id",
        "customer_id",
        "account_type",
        F.ceil("balance").alias("balance_whole"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

result = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        F.col("c.amount"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)
