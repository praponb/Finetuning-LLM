from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc_nulls_first())
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

ranked = (
    df_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .join(amount_by_account_id, on="account_id", how="left")
    .select("id", "account_id", "amount", "merchant", F.col("max_transaction_amount"))
    .orderBy(
        F.col("max_transaction_amount").asc_nulls_first(),
        F.col("merchant").desc(),
    )
)
