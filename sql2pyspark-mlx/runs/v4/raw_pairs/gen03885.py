from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.orderBy("opened_date", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

ranked = (
    df_accounts
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("balance_avg_3_prior", F.avg("balance").over(w4))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.amount"))
)

kept = (
    with_last
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.txn_time")), "left_semi")
)

result = (
    kept
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("amount_per_open"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("seq_no")).otherwise(0)).alias("credit_seq_no"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .filter(F.col("credit_seq_no") >= 0)
    .orderBy(F.col("week_start").desc_nulls_last())
)
