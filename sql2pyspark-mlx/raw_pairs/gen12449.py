from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("island_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.min("txn_time").alias("started_at"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("island_id")).otherwise(0)).alias("7_eleven_island_id"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("island_id")).otherwise(0)).alias("fee_island_id"),
    )
    .filter(F.col("fee_island_id") > 0)
)
