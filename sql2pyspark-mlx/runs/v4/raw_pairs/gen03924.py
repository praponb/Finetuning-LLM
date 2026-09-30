from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id")
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-12-31 23:00:00")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

result = (
    valid
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("amount_max_90d", F.max("amount").over(w2))
    .withColumn("is_flagged_flag_avg_30d", F.avg("is_flagged_flag").over(w3))
)
