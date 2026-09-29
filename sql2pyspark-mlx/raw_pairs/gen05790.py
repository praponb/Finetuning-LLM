from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("opened_date").desc())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-07-26").cast("date")) & F.col("customer_id").isin([34, 40]))
    .withColumn("status_filled", F.coalesce("status", F.lit("N/A")))
    .withColumn("adjusted_balance", F.round((F.col("balance") * 1.1), 2))
    .withColumn("status_clean", F.coalesce("status", F.lit("UNKNOWN")))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status_filled",
        "status_clean",
    )
)

valid = (
    ranked
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("account_type") == "CHECKING", 1).otherwise(0).alias("is_checking"),
        F.lower("account_type").alias("account_type_lower"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= "2025-05-03"), "left_anti")
)

result = (
    kept
    .groupBy("customer_id")
    .pivot("account_type", ["SAVINGS", "LOAN"])
    .agg(F.avg("balance").alias("value_sum"))
    .select(
        "customer_id",
        F.col("SAVINGS").alias("savings"),
        F.col("LOAN").alias("loan"),
    )
)
