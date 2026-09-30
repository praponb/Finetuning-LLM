from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("account_id", "txn_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy(F.year("txn_time"), "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("balance_bucket", "id")
w8 = Window.partitionBy(F.to_date("txn_time"), "merchant").orderBy(F.col("amount").asc_nulls_last())

with_dims = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.balance"))
)

metrics = (
    with_dims
    .filter(F.hour("txn_time").between(8, 11))
    .withColumn("amount_sum_7d", F.sum("amount").over(w))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w2))
    .withColumn("balance_bucket", F.ntile(5).over(w3))
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w4))
)

with_gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w5) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w6))

enriched = (
    islands
    .withColumn("next_balance", F.lead("balance", 2, 0).over(w7))
    .withColumn("amount_percent_rank", F.percent_rank().over(w8))
)

filtered = (
    enriched
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-06-04 06:00:00"))) & (F.col("amount_sum_7d") < 14299))
    .withColumn("session_no_plus_last_known_amount", (F.col("session_no") + F.col("last_known_amount")))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

result = (
    filtered
    .groupBy("txn_type")
    .pivot("merchant", ["Shell", "Lazada"])
    .agg(F.sum("amount_sum_7d"))
    .select("txn_type", F.col("Shell").alias("shell"), F.col("Lazada").alias("lazada"))
)
