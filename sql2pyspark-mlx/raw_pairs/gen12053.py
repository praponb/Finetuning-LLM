from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.amount").between(500, 1130))
    .select(
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("p.method"),
        F.col("p.order_id"),
    )
)
