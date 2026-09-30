from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("merchant")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

enriched = (
    df_transactions
    .withColumn("amount_avg_5", F.avg("amount").over(w))
    .withColumn("amount_max_90d", F.max("amount").over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)

result = (
    enriched
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "amount_avg_5",
        "amount_max_90d",
        "amount_share",
        F.when(F.col("merchant").rlike("^(Amazon|7-Eleven)$"), "CORE").otherwise("PRIORITY").alias("merchant_zone"),
        F.col("min_transaction_amount"),
    )
    .orderBy(F.col("amount").desc_nulls_last())
)
