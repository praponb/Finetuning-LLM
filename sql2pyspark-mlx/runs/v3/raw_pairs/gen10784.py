from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("payments.status") == "REFUNDED", F.col("payments.amount")).otherwise(0)).alias("refunded_amount"),
    )
    .filter(F.col("refunded_amount") > 35)
    .orderBy("refunded_amount", "opened_month")
    .limit(10)
)
