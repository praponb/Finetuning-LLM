from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.col("o.channel"),
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
    )
    .agg(F.max(F.col("p.amount")).alias("max_amount"))
    .orderBy(F.col("max_amount").desc())
)
