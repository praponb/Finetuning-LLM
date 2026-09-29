from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("status") == "FROZEN"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.length("account_type").alias("account_type_length"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.account_type_length",
        "s.resolved_status",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.txn_type"),
    )
)

joined = (
    with_last
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.is_active"))
    .select("a.*", F.col("b.city"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "DEBIT"), "left_anti")
)

with_last_accounts = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

with_dims = (
    with_last_accounts
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.balance",
        "src.account_type_length",
        "src.city",
        "src.transaction_txn_type",
        F.col("b.name"),
        F.col("b.segment"),
        F.col("b.signup_date"),
    )
)

not_flagged_accounts = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id") == 11), "left_anti")
)

result = (
    not_flagged_accounts
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "London", F.col("balance")).otherwise(0)).alias("london_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("account_type_length")).otherwise(0)).alias("consumer_account_type_length"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
        F.avg(F.when(F.col("city") == "Chiang Mai", F.col("balance"))).alias("chiang_mai_avg_balance"),
    )
    .filter(F.col("balance_stddev") >= 0)
    .orderBy(F.col("balance_stddev").asc_nulls_last())
)
