from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
        F.col("o.status"),
    )
    .agg(
        F.min(F.col("p.amount")).alias("min_amount"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("min_amount").desc())
)
