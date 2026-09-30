from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("p.method"),
        F.col("p.amount"),
        F.col("p.order_id"),
        F.col("o.shipping_fee"),
        F.when(F.col("o.amount") >= 1390, "High").when(F.col("o.amount") >= 270, "Medium").otherwise("Low").alias("amount_band"),
    )
)
