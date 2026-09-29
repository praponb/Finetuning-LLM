from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("balance", "id")
w3 = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("account_type", "status").orderBy(F.col("first_known_balance").desc_nulls_last(), "id")
w6 = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_first(), "id")

enriched = (
    df_accounts
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w_grp))
    .withColumn("prev_balance", F.lag("balance", 2, 0).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("status_max_balance", F.max("balance").over(w4))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < "2023-04-01"), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "status",
        "first_known_balance",
        "prev_balance",
        "row_seq",
        "status_max_balance",
        "latest_rank",
    )
    .orderBy(F.col("prev_balance").desc(), "id")
)
