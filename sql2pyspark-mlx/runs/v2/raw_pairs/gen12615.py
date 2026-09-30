from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.method").isin(["promptpay", "cash"]) | (F.col("p.status") == "FAILED"))
    .select(
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("p.amount"),
        F.col("p.method"),
    )
    .orderBy("order_date")
)
