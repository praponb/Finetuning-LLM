from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type")
w3 = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id")
w4 = Window.orderBy(F.col("account_type_max_balance").desc(), "id")

metrics = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w_part).alias("running_avg_balance"),
        F.max("balance").over(w_part).alias("running_max_balance"),
        F.count("balance").over(w_part).alias("running_count_balance"),
        F.max("balance").over(w2).alias("account_type_max_balance"),
    )
)

metrics_accounts = (
    metrics
    .withColumn("next_running_count_balance", F.lead("running_count_balance").over(w3))
    .withColumn("prev_balance", F.lag("balance").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("account_type_max_balance_bucket", F.ntile(5).over(w4))
)

eligible = (
    metrics_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < "2023-11-23"), "left_anti")
)

remaining_keys = (
    eligible
    .select("customer_id", "status")
    .exceptAll(
        df_accounts
        .filter(F.col("balance") <= 45000)
        .select("customer_id", "status")
    )
)

verified = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("customer_id")
    .pivot("status", ["FROZEN", "OPEN", "CLOSED"])
    .agg(F.avg("running_avg_balance"))
    .select(
        "customer_id",
        F.col("FROZEN").alias("frozen"),
        F.col("OPEN").alias("open"),
        F.col("CLOSED").alias("closed"),
    )
)
