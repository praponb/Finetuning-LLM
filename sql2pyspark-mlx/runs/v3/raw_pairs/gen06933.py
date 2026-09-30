from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.status").isin(["RETURNED", "COMPLETED"]))
    .select(F.col("o.id"), F.col("o.channel"))
    .orderBy("id")
)
