from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_first())
w5 = Window.orderBy(F.col("txn_time").desc(), "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("merchant_prefix", F.substring("merchant", 1, 2))
)

result = (
    valid
    .select(
        "*",
        F.sum("amount").over(w).alias("amount_sum_7d"),
        F.sum("amount").over(w2).alias("amount_sum_30d"),
        F.rank().over(w3).alias("amount_rank"),
        F.cume_dist().over(w4).alias("amount_cume_dist"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
