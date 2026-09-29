from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status")
w_recent = Window.partitionBy("account_type").orderBy("balance_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("txn_type").orderBy(F.col("credit_limit").desc(), "id")
w6 = Window.partitionBy("txn_type", "customer_id").orderBy("txn_time", "id")

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.col("balance").cast("int").alias("balance_int"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_5_prior"),
        F.min("balance").over(w2).alias("status_min_balance"),
        F.sum("balance").over(w_recent).alias("running_balance"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        "s.balance_avg_5_prior",
        F.col("c.txn_time"),
        F.col("c.txn_type"),
    )
)

joined = (
    with_latest
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.signup_date"), F.col("b.credit_limit"), F.col("b.age"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.city") == "Singapore")
    .select(
        "s.*",
        F.col("d.name"),
        F.col("d.signup_date").alias("customer_signup_date"),
    )
)

metrics = (
    with_dims
    .withColumn("credit_limit_bucket", F.ntile(10).over(w5))
    .withColumn("next_balance", F.lead("balance", 2).over(w6))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "opened_date",
        "txn_time",
        "txn_type",
        "signup_date",
        "credit_limit",
        "name",
        "customer_signup_date",
    )
    .orderBy(F.col("id").desc(), "opened_date")
)
