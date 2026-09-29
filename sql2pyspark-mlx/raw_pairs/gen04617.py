from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("txn_time").desc_nulls_last(), "id")

ranked = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
        F.stddev_samp("amount").alias("amount_stddev_3"),
    )
)
