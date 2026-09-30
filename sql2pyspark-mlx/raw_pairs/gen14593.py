from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("amount").desc(), "id")

with_dims = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        F.col("p.country"),
        F.col("p.city"),
        F.col("p.signup_date"),
    )
)

latest_match = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.country",
        "s.city",
        F.col("c.amount"),
        F.col("c.txn_type"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

with_windows = (
    with_last
    .select(
        "id",
        "customer_id",
        "country",
        "txn_type",
        F.min("balance").over(w_part).alias("last4_min_balance"),
        F.max("balance").over(w_part).alias("last4_max_balance"),
        F.avg("balance").over(w_part).alias("last4_avg_balance"),
        F.ntile(10).over(w3).alias("amount_bucket"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("country").isNotNull() & (~F.col("txn_type").eqNullSafe(F.lit("CREDIT"))))
    .select(
        "id",
        "customer_id",
        "country",
        "txn_type",
        "last4_min_balance",
        "last4_max_balance",
        "last4_avg_balance",
        F.when(F.col("country").isin(["DE", "TH"]), "Priority").otherwise("Other").alias("country_group"),
    )
)

matched_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.col("balance") < 66000)
        .select("id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "customer_id",
        "country",
        "txn_type",
        "last4_min_balance",
        "last4_avg_balance",
        "country_group",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("txn_type_zone"),
    )
    .orderBy("id", "last4_avg_balance")
)
