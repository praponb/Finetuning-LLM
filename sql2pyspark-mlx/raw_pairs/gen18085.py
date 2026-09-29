from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_last())
w3 = Window.partitionBy("account_type", "status").orderBy(F.col("balance_dense_rank").desc(), "id")
w4 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")
w5 = Window.partitionBy("status", "plan").orderBy(F.col("rank_in_group").desc())
w6 = Window.partitionBy("plan").orderBy(F.col("next_running_max_balance").desc(), "id", "subscription_id")

enriched = (
    df_accounts
    .withColumn("running_count_balance", F.count("balance").over(w_part))
    .withColumn("running_max_balance", F.max("balance").over(w_part))
    .withColumn("running_avg_balance", F.avg("balance").over(w_part))
    .withColumn("running_min_balance", F.min("balance").over(w_part))
    .withColumn("balance_dense_rank", F.dense_rank().over(w_recent))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched_accounts = (
    kept
    .filter(F.col("customer_id").isin([5, 34, 35, 38]))
    .withColumn("prev_balance_dense_rank", F.lag("balance_dense_rank").over(w4))
    .withColumn("next_running_max_balance", F.lead("running_max_balance").over(w4))
    .withColumn("prev_running_max_balance", F.lag("running_max_balance").over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
)

attributed = (
    enriched_accounts
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest = (
    attributed
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank_2", F.row_number().over(w6))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

pivoted = (
    ranked
    .groupBy("status", "plan")
    .pivot("account_type", ["SAVINGS", "LOAN"])
    .agg(
        F.max("next_running_max_balance").alias("total"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "status",
        "plan",
        F.col("SAVINGS_total").alias("savings_total"),
        F.col("SAVINGS_cnt").alias("savings_cnt"),
        F.col("LOAN_total").alias("loan_total"),
        F.col("LOAN_cnt").alias("loan_cnt"),
    )
)

result = (
    pivoted
    .select(
        "status",
        "plan",
        "savings_total",
        "savings_cnt",
        "loan_total",
        "loan_cnt",
        F.when(F.col("plan").rlike("^(standard|enterprise)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|premium)$"), "GROUP_A").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("plan").desc_nulls_last(), F.col("savings_cnt").desc())
)
