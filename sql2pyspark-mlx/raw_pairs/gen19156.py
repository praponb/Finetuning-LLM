from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w_part = Window.partitionBy("customer_id", "account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type", "account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_type", "customer_id").orderBy("txn_time", "id").rowsBetween(-2, -1)

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.amount") < 3900), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        F.col("c.txn_type"),
        F.col("c.txn_time"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (~F.col("x.is_flagged")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.txn_time")), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.last("txn_type", ignorenulls=True).over(w_part).alias("last_known_txn_type"),
    )
)

result = (
    metrics
    .select(
        "id",
        "last_known_txn_type",
        F.sum("seq_no").over(w4).alias("running_seq_no"),
        F.first("seq_no", ignorenulls=True).over(w5).alias("first_known_seq_no"),
        F.avg("seq_no").over(w6).alias("seq_no_avg_2_prior"),
    )
)
