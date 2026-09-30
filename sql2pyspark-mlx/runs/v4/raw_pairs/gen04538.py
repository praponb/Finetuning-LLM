from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("unassigned")))
    .withColumn("amount_tier", F.when(F.col("amount") >= 4200, "High").when(F.col("amount") >= 1100, "Medium").otherwise("Low"))
)

with_prev = base.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("merchant")
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("island_id")).otherwise(0)).alias("shell_island_id"),
    )
)
