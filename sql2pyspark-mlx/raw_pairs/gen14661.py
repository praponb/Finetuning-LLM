from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("is_flagged"))
    .withColumn("merchant_upper", F.upper("merchant"))
    .withColumn("txn_time_date", F.to_date("txn_time"))
)

result = (
    filtered
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time_date",
        F.min("amount").over(w).alias("running_min_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
    )
)
