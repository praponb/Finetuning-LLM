from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("balance", "id")
w4 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("balance_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("running_balance_2").desc(), "id")
w7 = Window.partitionBy("customer_id")

joined = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.id", "a.customer_id", "a.balance", "a.opened_date", F.col("p.email"))
)

with_windows = (
    joined
    .filter(F.col("balance") < 46000)
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("running_balance_2", F.sum("balance").over(w2))
    .withColumn("balance_bucket", F.ntile(3).over(w3))
    .withColumn("balance_avg_5", F.avg("balance").over(w4))
    .withColumn("balance_bucket_2", F.ntile(4).over(w3))
)

metrics = (
    with_windows
    .select(
        "id",
        "customer_id",
        "opened_date",
        "email",
        "balance_bucket",
        F.sum("balance_bucket").over(w5).alias("running_balance_bucket"),
        F.row_number().over(w6).alias("event_seq"),
        F.sum("balance_avg_5").over(w7).alias("customer_id_sum_balance_avg_5"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.to_date(F.col("ch.txn_time")) < "2025-05-01"), "left_anti")
)

grouped = (
    kept
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("customer_id_sum_balance_avg_5"), 2).alias("avg_customer_id_sum_balance_avg_5"),
    )
    .filter(F.col("row_count") > 1)
)

result = grouped.select("activity_month", "cnt", "avg_customer_id_sum_balance_avg_5")
