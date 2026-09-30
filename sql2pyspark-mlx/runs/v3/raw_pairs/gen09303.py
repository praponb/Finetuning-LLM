from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1030) & (F.col("paid_at") >= "2024-11-01") & (F.col("method") == "card"))
    .rollup(F.year("paid_at").alias("paid_year"), "method")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
