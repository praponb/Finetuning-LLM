from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy(F.col("balance_int").asc_nulls_last())
w3 = Window.partitionBy("transaction_merchant").orderBy(F.col("balance_int").desc())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-13").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.col("balance").cast("int").alias("balance_int"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

ranked = (
    with_latest
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "opened_date", "balance_int", "transaction_merchant")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .groupingSets(
        [["customer_id", "year_start"], ["customer_id"], []],
        "customer_id",
        "year_start",
    )
    .agg(
        F.sum("balance_int").alias("total_balance_int"),
        F.percentile_approx("balance_int", 0.95).alias("balance_int_p95"),
        F.count_if(F.col("transaction_merchant") == "7-Eleven").alias("7_eleven_events"),
        F.count_if(F.col("transaction_merchant") == "Shell").alias("shell_events"),
    )
)
