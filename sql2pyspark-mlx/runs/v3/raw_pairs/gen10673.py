from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount") > 970)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("c.credit_limit"),
        (F.col("c.age") - F.col("c.credit_limit")).alias("age_minus_credit_limit"),
    )
)
