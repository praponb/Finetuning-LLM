from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isNull() & (F.col("p.method") == "card"))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("discount").desc(), "id")
    .limit(5)
)
