from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-28 18:00:00").cast("timestamp")) & (F.col("txn_time") < "2023-09-01"))
    .select(
        "*",
        F.when(F.col("txn_type").isin(["REFUND", "CREDIT"]), "Core").otherwise("Other").alias("txn_type_group"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("amount") >= 3100, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
    )
    .orderBy(F.col("debit_rows").desc_nulls_first())
)
