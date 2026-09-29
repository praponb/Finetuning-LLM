from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([7, 26, 36]))
    .select(
        F.col("p.method"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("o.order_date"),
    )
)
