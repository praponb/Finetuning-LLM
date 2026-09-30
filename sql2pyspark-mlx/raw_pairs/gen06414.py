from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.sum(F.when(F.col("p.status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 1430)
    .orderBy(F.col("max_amount").desc())
)
