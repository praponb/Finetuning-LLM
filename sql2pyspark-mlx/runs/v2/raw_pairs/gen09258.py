from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") & (F.col("status") == "FAILED"))
    .groupBy(
        F.when(F.col("amount") >= 1440, "high").when(F.col("amount") >= 390, "mid").otherwise("low").alias("amount_bucket"),
        F.date_trunc("MONTH", "paid_at").alias("paid_month"),
        "status",
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("row_count"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("row_count").desc())
)
