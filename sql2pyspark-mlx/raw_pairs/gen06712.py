from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("amount_int_2", F.col("amount").cast("int"))
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "amount_int",
        "amount_int_2",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

enriched = (
    sessionized
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id").isNull())
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.txn_time",
        "a.amount_int_2",
        "a.session_id",
        F.col("b.status"),
        F.col("b.account_type"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.session_id",
        F.col("b.account_type").alias("account_account_type"),
        F.col("b.balance"),
    )
)

result = (
    joined
    .alias("src")
    .join(df_accounts.alias("dim"), F.col("src.account_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["status", "account_account_type"], ["status"], []],
        F.col("dim.status").alias("status"),
        F.col("src.account_account_type").alias("account_account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.amount")).alias("min_amount"),
    )
)
