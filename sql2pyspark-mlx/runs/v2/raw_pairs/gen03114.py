from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter((F.length(F.col("c.email")) >= 8) & (F.col("c.country") == "SG"))
    .select(
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.col("c.city"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
    )
)
