from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy(F.year("txn_time"), "merchant").orderBy(F.col("amount").desc())
w_grp = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("account_id").orderBy(F.col("running_max_amount_avg_5").desc(), "id")
w6 = Window.partitionBy("merchant", "account_id").orderBy("running_max_amount_avg_5", "id")
w7 = Window.partitionBy("merchant").orderBy("running_max_amount_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("running_max_amount_avg_5", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

metrics = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.avg("amount").over(w).alias("amount_avg_5"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "amount_cume_dist",
        F.max("amount_avg_5").over(w_grp).alias("running_max_amount_avg_5"),
        F.sum("amount_avg_5").over(w_grp).alias("running_sum_amount_avg_5"),
        F.count("amount_avg_5").over(w_grp).alias("running_count_amount_avg_5"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id") == 30)
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.merchant",
        "s.amount_cume_dist",
        "s.running_max_amount_avg_5",
        "s.running_sum_amount_avg_5",
        F.col("b.balance"),
    )
)

scored = (
    joined
    .select(
        "id",
        "account_id",
        "merchant",
        "amount_cume_dist",
        F.ntile(5).over(w5).alias("running_max_amount_avg_5_bucket"),
        F.ntile(3).over(w6).alias("running_max_amount_avg_5_bucket_2"),
        F.last("running_sum_amount_avg_5", ignorenulls=True).over(w7).alias("last_known_running_sum_amount_avg_5"),
        F.lead("balance", 2).over(w8).alias("next_balance"),
    )
)

result = (
    scored
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "merchant",
        "amount_cume_dist",
        "running_max_amount_avg_5_bucket",
        "last_known_running_sum_amount_avg_5",
        "next_balance",
        F.when(F.col("merchant").rlike("^(7-Eleven|Grab)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("merchant_zone"),
        F.col("avg_transaction_amount"),
    )
    .orderBy("merchant_zone")
)
