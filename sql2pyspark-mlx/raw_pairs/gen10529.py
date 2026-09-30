from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status")
w2 = Window.orderBy(F.col("abs_balance").desc_nulls_last())
w3 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") > F.lit("2023-05-01").cast("date")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.abs("balance").alias("abs_balance"),
    )
)

with_windows = (
    base
    .withColumn("status_stddev_samp_balance", F.stddev_samp("balance").over(w_part))
    .withColumn("status_max_balance", F.max("balance").over(w_part))
    .withColumn("abs_balance_cume_dist", F.cume_dist().over(w2))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w3))
)

matched_keys = (
    with_windows
    .select("status", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
        .select("status", "customer_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_semi")
)

kept_accounts = (
    kept
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_semi")
)

not_flagged = (
    kept_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type") == "FEE"), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "abs_balance",
        "status_stddev_samp_balance",
        "status_max_balance",
        "abs_balance_cume_dist",
        F.when(F.col("account_type").rlike("^(CHECKING|SAVINGS)$"), "OTHER_ZONE").when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "GROUP_A").otherwise("EXTENDED").alias("account_type_zone"),
    )
)
