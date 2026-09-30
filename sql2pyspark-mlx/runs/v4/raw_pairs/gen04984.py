from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("p.paid_at") >= "2023-12-01") & (F.col("o.channel") == "partner"))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("o.store_id"),
        F.col("c.age"),
        F.col("c.email"),
        F.concat(F.col("o.status"), F.lit(" "), F.col("o.channel")).alias("status_channel"),
    )
    .orderBy("store_id")
)
