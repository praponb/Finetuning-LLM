from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("segment", "account_type").orderBy(F.col("signup_date").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

latest = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept = (
    latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    kept
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.account_type",
        F.col("d.signup_date"),
        F.col("d.segment"),
        F.col("d.email"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        F.col("c.txn_type"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

enriched_accounts = (
    with_last
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.txn_type",
        F.col("d.signup_date"),
        F.col("d.name"),
        F.col("d.segment"),
    )
)

result = (
    enriched_accounts
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.count("*").alias("event_count"),
    )
)
