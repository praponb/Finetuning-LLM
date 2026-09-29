from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_first())
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").asc_nulls_last())
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc_nulls_first(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

cleaned = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 16))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("amount") >= 3600, "High").when(F.col("amount") >= 1600, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "txn_time", "is_flagged", "amount_tier")
)

deduped = (
    ranked
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_transactions = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount_tier",
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(20)
)
