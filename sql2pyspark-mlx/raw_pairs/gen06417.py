from pyspark.sql import functions as F

src = df_payments.filter(F.col("amount").between(580, 620))

summary = (
    src
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
)

result = summary.filter(F.col("max_amount") < 1410)
