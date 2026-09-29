from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    with_last
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.percentile_approx("transaction_amount", 0.9).alias("transaction_amount_p90"),
        F.percentile_approx("transaction_amount", [0.25, 0.5, 0.75]).alias("transaction_amount_quartiles"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
)
