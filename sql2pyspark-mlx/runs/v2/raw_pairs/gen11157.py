from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.channel"))
    .orderBy(F.col("amount").desc(), F.col("id").desc())
)
