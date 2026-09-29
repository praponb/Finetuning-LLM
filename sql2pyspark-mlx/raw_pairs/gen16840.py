from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(850, 1210))
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("amount_range").desc(), "order_id")
    .limit(5)
)
