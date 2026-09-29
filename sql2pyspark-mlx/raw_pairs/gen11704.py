from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "status")
w3 = Window.orderBy("next_balance", "id")
w_part = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").desc())
w5 = Window.partitionBy("account_type")
w6 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("next_balance").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_windows = (
    df_accounts
    .withColumn("next_balance", F.lead("balance").over(w))
    .withColumn("prev_balance", F.lag("balance").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .filter(F.col("next_balance") > 72000)
    .withColumn("opened_date_period_avg_prev_balance", F.avg("prev_balance").over(w2))
    .withColumn("prev_next_balance", F.lag("next_balance", 1, 0).over(w3))
    .withColumn("balance_cume_dist", F.cume_dist().over(w_part))
    .withColumn("account_type_max_next_balance", F.max("next_balance").over(w5))
    .withColumn("next_balance_bucket", F.ntile(3).over(w6))
)

eligible = (
    enriched
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

result = (
    latest_match
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "left")
    .cube(
        F.col("dim.segment").alias("segment"),
        F.col("dim.country").alias("country"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.balance")).alias("avg_balance"),
        F.max(F.col("s.prev_balance")).alias("max_prev_balance"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("status").desc())
)
