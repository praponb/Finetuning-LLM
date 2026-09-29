from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.country").isin(["JP", "UK", "TH"]) | F.col("o.status").isin(["CANCELLED", "RETURNED"]))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("c.credit_limit"),
    )
)
