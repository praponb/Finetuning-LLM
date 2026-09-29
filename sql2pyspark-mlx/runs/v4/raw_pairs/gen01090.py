from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.status") == "SUCCESS")
    .select(F.col("o.order_date"), F.col("o.channel"), F.col("p.status"))
    .orderBy(F.col("order_date").desc())
)
