from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.method").isin(["promptpay", "bank_transfer"]))
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.count(F.when(F.col("p.status") == "FAILED", 1)).alias("failed_count"),
        F.countDistinct(F.col("p.amount")).alias("unique_statuss"),
    )
    .orderBy("payment_count", "id")
)
