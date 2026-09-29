from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("discount").between(0.19, 0.25)).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.amount"))
    .orderBy(F.col("amount").desc(), "id")
    .limit(5)
)
