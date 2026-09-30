from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").asc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy("event_seq", "id").rowsBetween(-2, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

cleaned = (
    df_accounts
    .filter(F.col("opened_date").between("2023-06-26", "2024-04-10"))
    .select(
        "*",
        F.concat("account_type", F.lit(", "), "status").alias("account_type_status"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.hour(F.col("x.txn_time")).between(8, 12)), "left_anti")
)

metrics = (
    not_flagged
    .filter(F.col("balance") >= 68000)
    .withColumn("prev_balance", F.lag("balance").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("balance_dense_rank", F.dense_rank().over(w2))
)

base = (
    metrics
    .filter(F.col("status").eqNullSafe(F.lit("OPEN")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "account_type_status",
        "status_filled",
        "prev_balance",
        "event_seq",
        "balance_dense_rank",
        F.date_add("opened_date", 74).alias("opened_date_plus_74d"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled_2"),
    )
)

metrics_accounts = (
    base
    .withColumn("last3_sum_balance_dense_rank", F.sum("balance_dense_rank").over(w3))
    .withColumn("last3_avg_balance_dense_rank", F.avg("balance_dense_rank").over(w3))
    .withColumn("last3_max_balance_dense_rank", F.max("balance_dense_rank").over(w3))
)

result = (
    metrics_accounts
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "account_type_status",
        "prev_balance",
        "opened_date_plus_74d",
        "last3_sum_balance_dense_rank",
        F.col("min_order_discount"),
    )
    .orderBy(F.col("balance").asc_nulls_first())
)
