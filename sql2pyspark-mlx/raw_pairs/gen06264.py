from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.country"),
        F.col("c.name"),
        F.col("a.balance"),
        F.greatest(F.col("c.credit_limit"), F.col("c.age")).alias("greatest_credit_limit_age"),
    )
    .orderBy(F.col("balance").desc())
)
