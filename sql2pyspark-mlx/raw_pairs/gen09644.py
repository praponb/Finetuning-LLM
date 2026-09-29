from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("customer_id").isin([6, 25])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.city") == "Chiang Mai")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("age").desc())
)
