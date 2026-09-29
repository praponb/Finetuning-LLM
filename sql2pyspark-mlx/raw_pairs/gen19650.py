from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc_nulls_first(), "id")
w_recent = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("running_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id", "status").orderBy(F.col("running_balance").desc(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("status").orderBy(F.col("amazon_rows").desc())

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "account_type", "balance", "status")
)

latest_accounts = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest_accounts
    .withColumn("running_balance", F.sum("balance").over(w_recent))
    .withColumn("balance_avg_2_prior", F.avg("balance").over(w4))
    .withColumn("cumulative_balance", F.sum("balance").over(w_hist))
)

metrics = (
    scored
    .select(
        "*",
        F.sum("balance_avg_2_prior").over(w6).alias("running_balance_avg_2_prior"),
        F.ntile(3).over(w7).alias("running_balance_bucket"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

enriched = (
    with_last
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") != 50)
    .select("a.*", F.col("c.segment"), F.col("c.name"), F.col("c.signup_date"))
)

result = (
    enriched
    .groupBy(
        "status",
        F.when(F.col("running_balance") >= 424114, "LARGE").otherwise("SMALL").alias("running_balance_tier"),
    )
    .agg(
        F.sum(F.when(F.col("transaction_merchant") == "Grab", F.col("running_balance_avg_2_prior")).otherwise(0)).alias("grab_running_balance_avg_2_prior"),
        F.count(F.when(F.col("transaction_merchant") == "Amazon", 1)).alias("amazon_rows"),
        F.max(F.when(F.col("segment") == "SMB", F.col("running_balance"))).alias("smb_max_running_balance"),
        F.percentile_approx("running_balance", [0.25, 0.5, 0.75]).alias("running_balance_quartiles"),
        F.round(F.sum("running_balance_avg_2_prior") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("running_balance_avg_2_prior_per_open"),
    )
    .filter(F.col("amazon_rows") > 1)
    .withColumn("amazon_rows_rank", F.rank().over(w9))
    .orderBy(F.col("status").desc())
)
