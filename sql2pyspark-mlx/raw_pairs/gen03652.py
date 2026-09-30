from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.amount") <= 450) | (F.col("o.status") == "SHIPPED"))
    .select(
        F.col("o.discount"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.round(F.col("p.amount"), 1).alias("amount_rounded"),
    )
)
