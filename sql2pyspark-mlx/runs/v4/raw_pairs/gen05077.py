from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .filter(F.col("orders.channel").isin(["web", "mobile", "store"]) & (F.upper(F.trim(F.col("customers.name"))) == "IVY CHEN"))
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("customers.credit_limit"),
        F.col("customers.city"),
        (F.col("orders.discount") - F.col("orders.amount")).alias("discount_minus_amount"),
    )
)
