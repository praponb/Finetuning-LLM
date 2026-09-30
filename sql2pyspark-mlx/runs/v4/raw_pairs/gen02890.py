from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(600, 1300) & F.col("status").isin(["REFUNDED", "SUCCESS"]) & F.col("order_id").isin([3, 7]))
    .groupBy("method")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("num_payments"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 7300)
    .drop("total_amount")
    .orderBy(F.col("method").desc())
)
