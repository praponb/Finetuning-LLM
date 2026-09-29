from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "opened_date"), "account_type").orderBy("opened_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("transaction_merchant").orderBy(F.col("transaction_txn_time").asc_nulls_first(), "id")

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.when(F.col("account_type").isin(["LOAN", "SAVINGS"]), "Core").otherwise("Other").alias("account_type_group"),
    )
)

with_windows = (
    base
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("prev_balance", F.lag("balance", 2).over(w2))
)

kept = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.opened_date",
        "s.account_type_group",
        "s.running_balance",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

ranked = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "balance",
        "opened_date",
        "account_type_group",
        "running_balance",
        "transaction_txn_time",
    )
)

cleaned = (
    ranked
    .filter(F.col("opened_date") > F.lit("2023-06-28").cast("date"))
    .select(
        "id",
        "balance",
        "opened_date",
        "transaction_txn_time",
        F.hour("transaction_txn_time").alias("transaction_txn_time_hour"),
        F.floor("balance").alias("balance_whole"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("transaction_txn_time_hour") >= 89, "PREMIUM").otherwise("STANDARD").alias("transaction_txn_time_hour_tier"),
    )
    .agg(
        F.min("balance_whole").alias("min_balance_whole"),
        F.min("transaction_txn_time").alias("started_at"),
        ((F.unix_timestamp(F.max("transaction_txn_time")) - F.unix_timestamp(F.min("transaction_txn_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("min_balance_whole") < 1000000)
    .orderBy(F.col("duration_min").desc())
)
