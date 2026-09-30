from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.month("paid_at").alias("paid_month"),
        "method",
        F.to_date("paid_at").alias("paid_day"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 4)
)
