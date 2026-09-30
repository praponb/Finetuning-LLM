from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("amount_int", "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("merchant_length", F.length("merchant"))
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    latest
    .select("merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([1, 14, 20, 39]))
        .select("merchant")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.balance"), F.col("d.opened_date"))
)

result = (
    joined
    .select(
        "account_id",
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("balance").alias("BALANCE"),
    )
    .unpivot(["account_id"], ["AMOUNT_VAL", "BALANCE"], "metric", "reading")
    .filter(F.col("reading").isNotNull())
)
