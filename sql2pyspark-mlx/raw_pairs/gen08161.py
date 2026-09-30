from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").desc())
w2 = Window.orderBy("balance")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= F.to_timestamp(F.lit("2023-05-05 19:00:00"))))
    .select("*", F.round("amount", 1).alias("amount_rounded"))
)

joined = (
    valid
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.status") == "OPEN")
    .select(
        "s.id",
        "s.account_id",
        "s.merchant",
        "s.amount_rounded",
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)

matched_keys = (
    joined
    .select("merchant", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") >= "2025-03-01")
        .select("merchant", "account_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("balance_cume_dist", F.cume_dist().over(w))
    .withColumn("balance_percent_rank", F.percent_rank().over(w2))
)

result = (
    scored
    .select(
        "id",
        "merchant",
        "amount_rounded",
        "status",
        "account_type",
        "balance_percent_rank",
        F.when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "PRIORITY").otherwise("GROUP_A").alias("account_type_zone"),
    )
)
