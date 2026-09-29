from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount").between(520, 610))
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("p.status") == "FAILED", F.col("p.amount")).otherwise(0)).alias("failed_amount"),
    )
    .orderBy("failed_amount", "status")
    .limit(5)
)
