from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isin(["mobile", "partner"]))
    .select(
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
    )
    .orderBy("shipping_fee")
)
