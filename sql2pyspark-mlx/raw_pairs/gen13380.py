from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")
w_hist = Window.partitionBy(F.year("txn_time"), "txn_type").orderBy("amount", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.ntile(4).over(w).alias("amount_bucket"),
        F.avg("amount").over(w_hist).alias("amount_avg_2_prior"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_avg_2_prior",
        "cumulative_amount",
        F.when(F.col("merchant").rlike("^(Shell|Starbucks)$"), "EXTENDED").otherwise("CORE").alias("merchant_zone"),
    )
)
