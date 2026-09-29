from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("amount", "id")

with_windows = (
    df_transactions
    .withColumn("cumulative_amount", F.sum("amount").over(w))
    .withColumn("amount_avg_90d", F.avg("amount").over(w2))
    .withColumn("amount_count_90d", F.count("amount").over(w3))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w4))
    .withColumn("amount_bucket", F.ntile(4).over(w5))
)

result = (
    with_windows
    .groupBy("account_id")
    .agg(
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
    )
)
