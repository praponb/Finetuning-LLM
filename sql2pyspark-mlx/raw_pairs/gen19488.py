from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("is_credit", "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    ranked
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.balance"), F.col("d.account_type"), F.col("d.status"))
)

base_transactions = (
    with_dims
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= F.lit("2023-05-06 13:00:00").cast("timestamp")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_credit",
        "rn",
        "balance",
        "status",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

result = (
    base_transactions
    .select("id", "account_id", "txn_type", "txn_time", "merchant", "is_credit", "rn")
    .orderBy(F.col("is_credit").desc())
)
