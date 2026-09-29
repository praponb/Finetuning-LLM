from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.store_id").isin([22, 28]))
    .select(F.col("o.id"), F.col("o.discount"), F.col("o.store_id"), F.col("o.amount"))
    .orderBy(F.col("amount").desc())
)
