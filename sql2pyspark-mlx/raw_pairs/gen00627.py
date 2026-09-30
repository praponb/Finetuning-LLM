from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.discount") > 0.39)
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("p.status") == "FAILED", F.col("p.amount")).otherwise(0)).alias("failed_amount"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
    .orderBy("failed_amount")
)
