from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.order_id").isin([14, 18, 29]) | F.col("p.order_id").isNotNull())
    .select(
        F.col("p.order_id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("discount").desc())
)
