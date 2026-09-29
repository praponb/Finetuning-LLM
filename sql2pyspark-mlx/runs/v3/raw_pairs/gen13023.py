from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "card") & ~F.col("method").isin(["paypal", "cash"]))
    .groupBy("method")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .orderBy("method", F.col("success_count").desc())
)
