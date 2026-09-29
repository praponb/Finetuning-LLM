from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
    )
    .orderBy("method")
)
