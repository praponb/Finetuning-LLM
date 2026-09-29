from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("week_start").orderBy(F.col("shell_rows").desc())

deduped = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
    )
    .withColumn("shell_rows_rank", F.rank().over(w2))
    .orderBy(F.col("week_start").desc_nulls_last())
)
