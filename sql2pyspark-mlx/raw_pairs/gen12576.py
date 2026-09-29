from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") < 11700) & (F.year(F.col("c.signup_date")) == 2023))
    .select(
        F.col("s.id"),
        F.col("s.start_date"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.col("c.age"),
        F.concat_ws("/", F.col("c.country"), F.col("c.email")).alias("country_email_key"),
    )
    .orderBy("start_date")
)
