from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
)
