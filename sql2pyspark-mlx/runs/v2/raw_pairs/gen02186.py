from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "status",
        F.quarter("paid_at").alias("paid_quarter"),
        F.to_date("paid_at").alias("paid_day"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
    .filter(F.col("pct_refunded") < 47)
)
