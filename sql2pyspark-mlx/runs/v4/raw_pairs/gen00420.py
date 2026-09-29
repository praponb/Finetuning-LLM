from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_starbucks", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("merchant").isin(["7-Eleven", "Shell"]))
    .withColumn("is_starbucks", F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))
    .withColumn("txn_time_period", F.date_trunc("YEAR", "txn_time"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("UNKNOWN")))
)

result = (
    filtered
    .withColumn("amount_avg_3", F.avg("amount").over(w))
    .withColumn("last_known_is_starbucks", F.last("is_starbucks", ignorenulls=True).over(w2))
    .withColumn("amount_avg_90d", F.avg("amount").over(w3))
)
