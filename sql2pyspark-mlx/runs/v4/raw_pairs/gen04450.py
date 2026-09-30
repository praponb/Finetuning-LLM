from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy(F.col("amount").asc_nulls_first(), "id")

ranked = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .rollup(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount"))).alias("refund_total_amount"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("amount_per_fee"),
        F.max(F.when(F.col("txn_type") == "REFUND", F.col("amount"))).alias("refund_max_amount"),
    )
    .orderBy(F.col("num_merchants").desc_nulls_first())
)
