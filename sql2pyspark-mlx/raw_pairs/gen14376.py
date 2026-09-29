from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "*",
        F.avg("amount").over(w_running).alias("running_avg_amount"),
        F.max("amount").over(w_running).alias("running_max_amount"),
        F.min("amount").over(w_running).alias("running_min_amount"),
        F.avg("amount").over(w2).alias("amount_avg_7d"),
    )
)
