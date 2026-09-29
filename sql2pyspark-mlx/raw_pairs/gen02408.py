from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.lower("status").alias("status_lower"),
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select("s.*", F.col("b.country"), F.col("b.email"), F.col("b.name"))
)

cleaned = (
    joined
    .filter(F.col("opened_date") >= F.lit("2023-03-23").cast("date"))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status_account_type_key",
        "country",
        "email",
        "name",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status_account_type_key",
        "s.country",
        "s.name",
        "s.balance_filled",
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "country",
        "transaction_txn_time",
        F.when(F.col("country").rlike("^(TH|DE)$"), "PRIORITY").when(F.col("country").rlike("^(US|SG)$"), "GROUP_B").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("country").desc_nulls_first())
)
