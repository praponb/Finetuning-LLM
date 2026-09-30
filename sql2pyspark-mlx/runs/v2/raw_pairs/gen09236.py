from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.country").isin(["DE", "TH"]) | F.col("c.name").like("Ca%"))
    .select(
        F.col("p.paid_at"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("c.name"),
        F.add_months(F.col("o.order_date"), 1).alias("order_date_plus_months"),
    )
    .orderBy("name", "customer_id")
)
