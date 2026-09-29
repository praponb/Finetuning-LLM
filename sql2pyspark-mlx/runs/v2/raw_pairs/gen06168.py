from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("p.order_id"),
        F.col("o.order_date"),
        F.substring(F.col("p.method"), 1, 4).alias("method_prefix"),
    )
    .orderBy("method_prefix", F.col("order_date").desc())
)
