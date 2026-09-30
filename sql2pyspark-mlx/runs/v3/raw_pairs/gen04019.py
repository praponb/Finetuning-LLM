from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("c.signup_date") > "2023-05-20") & (F.col("c.city") == "Chiang Mai"))
    .select(F.col("s.id"), F.col("s.status"), F.col("c.credit_limit"))
)
