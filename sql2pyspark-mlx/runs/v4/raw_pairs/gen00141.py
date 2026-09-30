from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("amount")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= "2024-04-01"))
    .withColumn("clean_txn_type", F.trim("txn_type"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    valid
    .select(
        "*",
        F.ntile(4).over(w).alias("amount_bucket"),
        F.sum("amount").over(w2).alias("running_amount"),
        F.last("amount", ignorenulls=True).over(w3).alias("last_known_amount"),
        F.count("amount").over(w4).alias("amount_count_30d"),
        F.dense_rank().over(w5).alias("amount_dense_rank"),
    )
)
