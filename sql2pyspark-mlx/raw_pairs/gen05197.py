from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isin([4, 17, 26, 27]) | F.col("p.amount").between(770, 1310))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("p.status"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("o.order_date"),
    )
    .orderBy("amount", "order_date")
)
