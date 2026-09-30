from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2024-01-01") & (F.col("order_id") == 24))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
    .orderBy(F.col("paid_quarter").desc())
)
