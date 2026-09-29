from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.col("a.status"),
        F.quarter(F.col("a.opened_date")).alias("opened_quarter"),
    )
    .agg(F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 35)
    .orderBy(F.col("opened_quarter").desc(), F.col("stddev_amount").desc())
)
