from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(F.count(F.when(F.col("payments.method") == "card", 1)).alias("card_count"))
)
