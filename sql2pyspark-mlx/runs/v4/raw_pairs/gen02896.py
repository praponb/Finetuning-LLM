from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance_adjusted", "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")
w_grp = Window.partitionBy("status", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("balance", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w6 = Window.partitionBy("customer_id").orderBy("balance", "id")
w7 = Window.partitionBy("customer_id", "account_type").orderBy("row_seq", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("row_seq").desc())
w10 = Window.partitionBy("account_type").orderBy(F.col("row_seq_2").desc(), "id")
w11 = Window.partitionBy("status", "account_type")

valid = (
    df_accounts
    .filter(F.col("customer_id") == 19)
    .withColumn("balance_adjusted", F.round((F.col("balance") * 1.1), 2))
)

metrics = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("cumulative_balance_adjusted", F.sum("balance_adjusted").over(w_grp))
    .withColumn("balance_adjusted_avg_5_prior", F.avg("balance_adjusted").over(w4))
)

with_windows = (
    metrics
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.ntile(5).over(w6).alias("balance_bucket"),
        F.avg("balance").over(w7).alias("balance_avg_3"),
        F.sum("balance_adjusted_avg_5_prior").over(w8).alias("running_balance_adjusted_avg_5_prior"),
        F.cume_dist().over(w9).alias("row_seq_cume_dist"),
    )
)

result = (
    with_windows
    .withColumn("row_seq_2_bucket", F.ntile(3).over(w10))
    .withColumn("status_avg_row_seq", F.avg("row_seq").over(w11))
)
