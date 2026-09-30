from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.filter(F.col("customer_id").isNotNull()).alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "left_semi")
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customers.age"),
    )
    .orderBy(F.col("age").desc())
)
