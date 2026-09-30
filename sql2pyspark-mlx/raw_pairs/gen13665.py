from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_windows = (
    df_accounts
    .filter(F.col("balance").between(17000, 53000))
    .withColumn("prev_balance", F.lag("balance").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("row_seq_2", F.row_number().over(w2))
)

enriched = (
    with_windows
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("a.*", F.col("c.country"))
)

kept = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([2, 4, 28, 33])), "left_semi")
)

joined = (
    kept
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.email"))
)

latest_match = (
    joined
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.opened_date",
        "s.prev_balance",
        "s.row_seq",
        F.col("c.merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

matched_keys = (
    latest_match
    .select("id", "customer_id")
    .intersectAll(
        df_accounts
        .filter(~F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]))
        .select("id", "customer_id")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched_accounts = (
    kept_rows
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.credit_limit"), F.col("c.country"), F.col("c.segment"))
)

result = (
    enriched_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "prev_balance",
        "row_seq",
        "merchant",
        "credit_limit",
        F.when(F.col("merchant").rlike("^(7-Eleven|Lazada)$"), "GROUP_B").when(F.col("merchant").rlike("^(Grab|Amazon)$"), "GROUP_A").otherwise("PRIORITY").alias("merchant_zone"),
    )
)
