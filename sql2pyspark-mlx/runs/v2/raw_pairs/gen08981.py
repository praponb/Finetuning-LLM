from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("shipping_fee") < 39).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.amount").between(650, 1430))
    .select(F.col("p.id"), F.col("p.method"), F.col("p.amount"), F.col("p.order_id"))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(10)
)
