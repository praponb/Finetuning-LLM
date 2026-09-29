from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .select(
        F.col("o.channel"),
        F.col("p.paid_at"),
        F.round((F.col("p.amount") * 1.07), 2).alias("amount_adjusted"),
    )
    .orderBy("amount_adjusted")
)
