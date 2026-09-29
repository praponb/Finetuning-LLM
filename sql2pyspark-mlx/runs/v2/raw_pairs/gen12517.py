from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([28, 37]) & F.col("o.store_id").isNull())
    .select(
        F.col("p.method"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.when(F.col("p.method") == "card", 1).otherwise(0).alias("is_card"),
    )
)
