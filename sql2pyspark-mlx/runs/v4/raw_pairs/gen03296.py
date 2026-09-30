from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id")
w2 = Window.partitionBy("txn_type").orderBy("amount", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.to_date("txn_time").alias("txn_time_date"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

result = (
    valid
    .withColumn("merchant_stddev_samp_amount", F.stddev_samp("amount").over(w))
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
    .withColumn("cumulative_amount", F.sum("amount").over(w3))
)
