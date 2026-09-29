from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left")
    .filter(F.upper(F.trim(F.col("customers.email"))) == "CAROL@COMPANY.CO")
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.start_date"),
        F.col("customers.signup_date"),
        F.col("customers.city"),
        F.col("customers.country"),
    )
    .orderBy(F.col("start_date").desc())
)
