from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2023-07-09")
    .groupBy(
        F.date_format("paid_at", "yyyy-MM").alias("paid_month"),
        "method",
        F.hour("paid_at").alias("paid_hour"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
        F.avg("amount").alias("avg_amount"),
    )
)
