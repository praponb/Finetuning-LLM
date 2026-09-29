from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy(F.col("merchant_length").desc(), "id")
w_ordered = Window.orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_ordered_2 = Window.orderBy("merchant_length", "id").rowsBetween(-2, -1)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("merchant_length", F.length("merchant"))
    .withColumn("clean_merchant", F.trim("merchant"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("UNKNOWN")))
)

result = (
    filtered
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "merchant_length",
        F.ntile(5).over(w).alias("merchant_length_bucket"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("amount").over(w3).alias("amount_sum_30d"),
        F.avg("amount").over(w_ordered_2).alias("amount_avg_2_prior"),
    )
)
