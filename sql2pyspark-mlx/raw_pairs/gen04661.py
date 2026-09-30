from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_avg_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .filter(F.col("account_id").isin([1, 8, 31, 35]))
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "row_num",
        F.avg("amount").over(w2).alias("running_avg_amount"),
        F.count("amount").over(w2).alias("running_count_amount"),
        F.max("amount").over(w2).alias("running_max_amount"),
    )
)

scored_transactions = (
    scored
    .select(
        "id",
        "is_flagged",
        "running_avg_amount",
        "running_max_amount",
        F.avg("running_max_amount").over(w3).alias("running_avg_running_max_amount"),
        F.sum("running_max_amount").over(w3).alias("running_sum_running_max_amount"),
        F.count("running_max_amount").over(w3).alias("running_count_running_max_amount"),
        F.max("running_max_amount").over(w3).alias("running_max_running_max_amount"),
    )
)

result = (
    scored_transactions
    .select("id", "running_sum_running_max_amount", "running_count_running_max_amount")
    .orderBy(F.col("running_count_running_max_amount").asc_nulls_first())
)
