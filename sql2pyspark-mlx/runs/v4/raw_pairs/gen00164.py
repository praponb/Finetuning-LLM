from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time_hour").desc(), "id")
w_ordered = Window.partitionBy("merchant", "txn_type")
w3 = Window.partitionBy("merchant").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("amount", "id")

cleaned = (
    df_transactions
    .filter(F.col("account_id").isin([13, 17]))
    .select("*", F.hour("txn_time").alias("txn_time_hour"))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.account_type"), F.col("p2.status"))
)

latest = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.min("txn_time_hour").over(w_ordered).alias("merchant_min_txn_time_hour"),
        F.sum("txn_time_hour").over(w3).alias("running_txn_time_hour"),
        F.lead("txn_time_hour", 1).over(w4).alias("next_txn_time_hour"),
    )
)

result = (
    with_windows
    .groupBy("merchant", "status")
    .pivot("account_type", ["CHECKING", "CREDIT", "LOAN"])
    .agg(F.avg("amount").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "merchant",
        "status",
        F.col("CHECKING_amount").alias("checking_amount"),
        F.col("CHECKING_n").alias("checking_n"),
        F.col("CREDIT_amount").alias("credit_amount"),
        F.col("CREDIT_n").alias("credit_n"),
        F.col("LOAN_amount").alias("loan_amount"),
        F.col("LOAN_n").alias("loan_n"),
    )
)
