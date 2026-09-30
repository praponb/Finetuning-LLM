from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.year("shipped_date") == 2025).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.channel"), F.col("o.status"), F.col("o.amount"))
    .orderBy(F.col("amount").desc(), F.col("status").desc())
)
