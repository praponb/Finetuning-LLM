from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc_nulls_last(), "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.trim("txn_type").alias("clean_txn_type"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id", "txn_time")
)
