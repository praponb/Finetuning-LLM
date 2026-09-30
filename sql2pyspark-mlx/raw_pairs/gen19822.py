from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("amount_stddev").desc())

result = (
    df_transactions
    .groupBy(
        F.date_trunc("quarter", "txn_time").alias("quarter_start"),
        F.when(F.col("amount") >= 4100, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.min("txn_time").alias("started_at"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .withColumn("amount_stddev_rank", F.rank().over(w))
    .orderBy(F.col("fee_amount").asc_nulls_first())
)
