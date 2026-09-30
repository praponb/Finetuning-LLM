from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("c.country"),
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.round((F.col("c.credit_limit") / F.col("c.age")), 2).alias("credit_limit_per_age"),
    )
    .orderBy("opened_date")
)
