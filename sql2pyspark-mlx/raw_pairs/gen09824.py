from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.orderBy("amount", "id")

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-07-18").cast("date"))
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.when(F.col("status").isin(["OPEN", "FROZEN"]), "Priority").otherwise("Other").alias("status_group"),
        F.upper("status").alias("status_upper"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "opened_date", "status_group", "status_upper")
)

with_last = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"), F.col("c.amount"))
)

scored = (
    with_last
    .filter(F.col("amount").between(2400, 2500))
    .select(
        "id",
        "status_group",
        "status_upper",
        "transaction_txn_time",
        F.lead("amount").over(w3).alias("next_amount"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    scored
    .select("id", "status_group", "transaction_txn_time", "next_amount", "row_seq")
)
