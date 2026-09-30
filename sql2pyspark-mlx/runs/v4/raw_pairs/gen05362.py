from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

result = (
    with_last
    .withColumn("year_start", F.date_trunc("YEAR", "transaction_txn_time"))
    .groupingSets(
        [["customer_id", "year_start"], ["customer_id"], []],
        "customer_id",
        "year_start",
    )
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_per_credit"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
