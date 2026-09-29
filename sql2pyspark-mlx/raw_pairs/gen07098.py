from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("abs_balance").desc(), "id")
w_recent = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("opened_date"), "customer_id").orderBy("running_avg_balance", "id")
w4 = Window.partitionBy("customer_id", "account_type")
w5 = Window.partitionBy("account_type").orderBy("row_num", "id")
w6 = Window.orderBy("running_avg_balance", "id")
w7 = Window.partitionBy("account_type").orderBy(F.col("running_avg_balance").desc_nulls_first())
w8 = Window.partitionBy("customer_id").orderBy("abs_balance", "id")
w9 = Window.partitionBy(F.year("opened_date"), "customer_id").orderBy("customer_id_sum_abs_balance", "id").rowsBetween(-3, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

valid = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.concat("account_type", F.lit(" - "), "status").alias("account_type_status"),
        F.abs("balance").alias("abs_balance"),
        F.coalesce("status", F.lit("NONE")).alias("status_filled"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .withColumn("running_count_balance", F.count("balance").over(w_recent))
    .withColumn("running_max_balance", F.max("balance").over(w_recent))
    .withColumn("running_avg_balance", F.avg("balance").over(w_recent))
)

scored = (
    metrics
    .withColumn("next_running_max_balance", F.lead("running_max_balance", 1).over(w3))
    .withColumn("customer_id_sum_abs_balance", F.sum("abs_balance").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("prev_running_avg_balance", F.lag("running_avg_balance", 1).over(w6))
    .withColumn("running_avg_balance_dense_rank", F.dense_rank().over(w7))
)

enriched = (
    scored
    .withColumn("next_customer_id_sum_abs_balance", F.lead("customer_id_sum_abs_balance", 1, 0).over(w8))
    .withColumn("seq_no_avg_3", F.avg("seq_no").over(w9))
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "account_type_status",
        "abs_balance",
        "running_count_balance",
        "running_avg_balance",
        F.col("avg_subscription_monthly_fee"),
    )
)
