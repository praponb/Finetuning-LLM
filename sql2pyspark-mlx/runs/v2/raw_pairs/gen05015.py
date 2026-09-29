from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .filter(F.col("p.order_id").isNotNull())
    .join(df_orders.filter(F.col("customer_id").isNull()).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.amount") < 650)
    .select(F.col("p.id"), F.col("p.status"))
    .orderBy("status", "id")
    .limit(3)
)
