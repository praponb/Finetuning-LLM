from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-08-07")))
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

filtered = (
    cleaned
    .filter(F.col("status").isNotNull() & (F.col("balance") < 65000))
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "account_type_prefix",
        "resolved_status",
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "Group A").otherwise("Other").alias("status_group"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.merchant"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "status",
        "account_type_prefix",
        "resolved_status",
        "status_group",
        "status_clean",
        "row_num",
        "transaction_txn_time",
        "merchant",
        F.when(F.col("merchant").rlike("^(7-Eleven|Lazada)$"), "GROUP_B").when(F.col("merchant").rlike("^(Grab|Amazon)$"), "CORE").otherwise("EXTENDED").alias("merchant_zone"),
    )
)
