from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.store_id"))
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
)
