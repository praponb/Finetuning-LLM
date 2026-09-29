from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("p.method") == "card")
    .groupBy(
        F.when(F.col("o.status").isin(["CANCELLED", "RETURNED"]), "core").otherwise("other").alias("status_group"),
        F.month(F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        F.sum(F.col("p.amount")).alias("total_amount"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
)
