from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "account_type").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("balance", "id")
w4 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

metrics = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 3, 11]))
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_2"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.lead("balance", 1, 0).over(w3).alias("next_balance"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.account_type",
        "a.balance",
        "a.balance_avg_2",
        F.col("d.signup_date"),
        F.col("d.city"),
        F.col("d.name"),
    )
)

metrics_accounts = (
    with_dims
    .select(
        "id",
        "account_type",
        "balance",
        "name",
        F.max("balance").over(w4).alias("running_max_balance"),
        F.min("balance").over(w4).alias("running_min_balance"),
        F.avg("balance").over(w4).alias("running_avg_balance"),
    )
)

eligible = (
    metrics_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.amount").isNotNull()), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.name",
        "s.running_max_balance",
        "s.running_avg_balance",
        F.col("c.amount").alias("transaction_amount"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type") == "DEBIT"), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("running_max_balance") >= 58000, "HIGH").otherwise("LOW").alias("running_max_balance_tier"),
    )
    .agg(
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.count_if(F.col("account_type") == "LOAN").alias("loan_events"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
    )
)
