from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_last())

scored = (
    df_transactions
    .withColumn("last7_sum_amount", F.sum("amount").over(w))
    .withColumn("last7_min_amount", F.min("amount").over(w))
    .withColumn("last7_count_amount", F.count("amount").over(w))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
)

result = (
    scored
    .groupBy("merchant")
    .agg(
        F.round(F.avg("amount_dense_rank"), 2).alias("avg_amount_dense_rank"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.min("amount").alias("min_amount"),
        F.avg(F.when(F.col("txn_type") == "FEE", F.col("last7_sum_amount"))).alias("fee_avg_last7_sum_amount"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount_dense_rank"))).alias("amazon_total_amount_dense_rank"),
    )
    .orderBy(F.col("fee_avg_last7_sum_amount").desc())
)
