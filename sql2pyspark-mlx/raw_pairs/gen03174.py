from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-08-01")
    .select(
        "id",
        "amount",
        F.row_number().over(w_running).alias("seq_no"),
        F.avg("amount").over(w2).alias("amount_avg_2_prior"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
    )
)

result = (
    scored
    .select("id", "amount_avg_2_prior")
    .orderBy(F.col("amount_avg_2_prior").asc_nulls_last(), F.col("id").desc())
)
