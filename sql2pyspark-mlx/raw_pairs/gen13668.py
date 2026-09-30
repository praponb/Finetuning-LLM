from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.city").isin(["Bangkok", "London", "Chiang Mai"]))
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("signup_date").desc())
)
