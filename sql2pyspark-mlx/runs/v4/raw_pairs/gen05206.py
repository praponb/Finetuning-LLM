from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id")
w4 = Window.orderBy(F.col("txn_time_year").desc(), "id")
w5 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy("txn_time_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("txn_time_year_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("txn_type")

filtered = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .withColumn("txn_time_year", F.year("txn_time"))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

enriched = (
    sessionized
    .withColumn("amount_bucket", F.ntile(3).over(w3))
    .withColumn("txn_time_year_bucket", F.ntile(4).over(w4))
)

result = (
    enriched
    .withColumn("amount_bucket_sum_30d", F.sum("amount_bucket").over(w5))
    .withColumn("last_known_txn_time_year", F.last("txn_time_year", ignorenulls=True).over(w6))
    .withColumn("running_amount", F.sum("amount").over(w7))
    .withColumn("txn_type_sum_txn_time_year_bucket", F.sum("txn_time_year_bucket").over(w8))
)
