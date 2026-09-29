from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter((F.col("c.signup_date") < "2023-03-01") | (F.col("o.discount") <= 0.21))
    .select(
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("c.name"),
        F.col("o.amount"),
        F.datediff(F.current_date(), F.col("c.signup_date")).alias("days_since_signup_date"),
    )
    .orderBy(F.col("signup_date").desc(), F.col("amount").desc())
)
