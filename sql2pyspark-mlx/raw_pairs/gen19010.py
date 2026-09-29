from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("a.balance"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("c.age"),
    )
    .orderBy("age", "signup_date")
)
