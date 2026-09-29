from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-5, -1)

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-08-08 02:00:00").cast("timestamp")) & F.col("is_flagged"))
    .withColumn("txn_type_lower", F.lower("txn_type"))
    .withColumn("clean_txn_type", F.trim("txn_type"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    filtered
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        "txn_type_lower",
        "clean_txn_type",
        F.count("amount").over(w).alias("amount_count_7d"),
        F.avg("amount").over(w2).alias("amount_avg_5_prior"),
    )
)
