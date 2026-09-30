from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"), F.col("c.city"))
    .agg(
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
        F.sum(F.when(F.col("p.method") == "card", F.col("p.amount")).otherwise(0)).alias("card_amount"),
    )
    .filter(F.col("card_amount") > 47)
)
