from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("txn_type")
w4 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy("txn_time", "id")

scored = (
    df_transactions
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_2_prior"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.count("amount").over(w3).alias("txn_type_count_amount"),
        F.avg("amount").over(w4).alias("amount_avg_7d"),
        F.lag("amount", 2, 0).over(w5).alias("prev_amount"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("activity_month"))
    .agg(
        F.min("txn_time").alias("started_at"),
        F.round(F.avg("amount_bucket"), 2).alias("avg_amount_bucket"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
    )
    .orderBy("credit_count")
)
