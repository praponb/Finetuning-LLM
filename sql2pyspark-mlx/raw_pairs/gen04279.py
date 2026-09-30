from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") >= 32)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("o.channel"),
        F.col("c.country"),
        F.initcap(F.trim(F.col("p.method"))).alias("method_title"),
    )
    .orderBy("order_id", "id")
    .limit(1)
)
