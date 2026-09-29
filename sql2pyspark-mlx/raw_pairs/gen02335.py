from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(~(F.col("p.status").isin(["FAILED", "REFUNDED", "SUCCESS"]) | (F.col("p.method") != "bank_transfer")))
    .select(
        F.col("p.method"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        (F.col("o.amount") - F.col("o.discount")).alias("amount_minus_discount"),
    )
)
