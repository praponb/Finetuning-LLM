from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("merchant").orderBy("is_savings", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("credit_limit", "id").rowsBetween(-2, -1)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.trim("account_type").alias("clean_account_type"),
        F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0).alias("is_savings"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

with_dims = (
    kept
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.is_savings",
        F.col("p.segment"),
        F.col("p.credit_limit"),
        F.col("p.signup_date"),
    )
)

with_latest = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.merchant"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.signup_date")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "customer_id",
        "segment",
        "txn_time",
        "merchant",
        F.min("is_savings").over(w_running).alias("last4_min_is_savings"),
        F.count("is_savings").over(w_running).alias("last4_count_is_savings"),
        F.max("is_savings").over(w_running).alias("last4_max_is_savings"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_2_prior"),
    )
)

with_dims_accounts = (
    with_windows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.segment",
        "a.merchant",
        "a.last4_count_is_savings",
        "a.credit_limit_avg_2_prior",
        F.col("d.city"),
    )
)

wide = (
    with_dims_accounts
    .groupBy("customer_id")
    .pivot("merchant", ["Lazada", "Starbucks", "Amazon"])
    .agg(
        F.avg("credit_limit_avg_2_prior").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "customer_id",
        F.col("Lazada_value_sum").alias("lazada_value_sum"),
        F.col("Lazada_item_count").alias("lazada_item_count"),
        F.col("Starbucks_value_sum").alias("starbucks_value_sum"),
        F.col("Starbucks_item_count").alias("starbucks_item_count"),
        F.col("Amazon_value_sum").alias("amazon_value_sum"),
        F.col("Amazon_item_count").alias("amazon_item_count"),
    )
)

result = (
    wide
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "lazada_value_sum",
        "lazada_item_count",
        "starbucks_item_count",
        "amazon_value_sum",
        "amazon_item_count",
        F.col("sum_account_balance"),
    )
    .orderBy(
        F.col("lazada_value_sum").desc_nulls_first(),
        F.col("lazada_item_count").desc(),
    )
)
