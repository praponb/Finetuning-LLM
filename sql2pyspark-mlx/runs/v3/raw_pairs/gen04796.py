from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(~F.col("p.method").isin(["bank_transfer", "paypal"]) & (F.col("o.amount") < 1220))
    .select(
        F.col("p.status"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("method").desc())
)
