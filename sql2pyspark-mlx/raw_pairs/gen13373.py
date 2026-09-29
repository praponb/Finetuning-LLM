from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy(F.date_trunc("WEEK", "txn_time")).orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "REFUND"]))
    .select(
        "*",
        F.max("amount").over(w).alias("amount_max_90d"),
        F.sum("amount").over(w2).alias("running_amount"),
        F.avg("amount").over(w3).alias("amount_avg_2_prior"),
        F.max("amount").over(w4).alias("amount_max_7d"),
    )
)
