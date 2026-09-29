from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("distinct_account_ids").desc())

kept_keys = (
    df_transactions
    .select("merchant", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("account_id") == 32)
        .select("merchant", "account_id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "merchant",
        F.when(F.col("amount") >= 3400, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.max("txn_time").alias("last_seen"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .withColumn("distinct_account_ids_rank", F.rank().over(w))
    .orderBy(F.col("amount_tier").desc_nulls_last())
)
