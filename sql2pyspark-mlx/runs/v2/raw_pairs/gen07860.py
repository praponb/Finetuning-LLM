from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.status").isin(["SUCCESS", "FAILED", "REFUNDED"]) | F.col("o.shipping_fee").between(19, 27))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("o.shipping_fee"),
        F.col("o.amount").alias("order_amount"),
        F.greatest(F.col("o.discount"), F.col("o.amount")).alias("greatest_discount_amount"),
    )
    .orderBy(F.col("order_amount").desc(), F.col("shipping_fee").desc())
)
