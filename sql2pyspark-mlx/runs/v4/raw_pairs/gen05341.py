from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_methods"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.unique_methods") > 3)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("os.unique_methods"),
    )
    .orderBy("id")
    .limit(10)
)
