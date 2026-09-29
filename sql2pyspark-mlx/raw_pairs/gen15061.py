from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("merchant").isin(["Starbucks", "Shell", "Lazada"]))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_int",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_transactions
    .groupBy("merchant")
    .agg(
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("amount_int", 0.5).alias("amount_int_p50"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
    )
)
