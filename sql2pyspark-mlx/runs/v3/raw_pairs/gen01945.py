from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.customer_id").isin([17, 26, 27]))
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.countDistinct(F.col("p.amount")).alias("unique_statuss"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .filter(F.col("unique_statuss") >= 2)
    .orderBy(F.col("max_amount").desc())
)
