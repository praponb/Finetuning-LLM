from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.order_id") == 16)
    .select(F.col("p.id"), F.col("p.order_id"), F.col("o.status"))
    .orderBy(F.col("id").desc())
)
