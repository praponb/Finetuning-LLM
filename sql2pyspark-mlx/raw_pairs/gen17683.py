from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "account_type")
w2 = Window.partitionBy("city").orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("balance").desc())

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-09-14").cast("date")) & (F.col("balance") > 26000))
    .withColumn("balance_int", F.col("balance").cast("int"))
)

remaining_keys = (
    base
    .select("status", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "SAVINGS")
        .select("status", "customer_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(F.lower(F.trim(F.col("d.name"))) == "jack taylor")
    .select("a.*", F.col("d.city"))
)

with_windows = (
    joined
    .select(
        "*",
        F.stddev_samp("balance").over(w).alias("opened_date_month_stddev_samp_balance"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
        F.percent_rank().over(w3).alias("balance_percent_rank"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_int",
        "opened_date_month_stddev_samp_balance",
        "balance_dense_rank",
        "balance_percent_rank",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "GROUP_B").when(F.col("status").rlike("^(CLOSED)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy("account_type")
)
