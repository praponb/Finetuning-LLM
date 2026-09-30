from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"), "status", "order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("max_amount") >= 710)
    .orderBy(F.col("paid_year").desc(), "status", "order_id")
    .limit(1)
)
