from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.lower(F.trim(F.col("c.email"))) == "david.lee@gmail.com") & F.col("c.name").isNotNull())
    .select(
        F.col("c.is_active"),
        F.col("c.name"),
        F.col("c.credit_limit"),
        F.col("a.balance"),
    )
    .orderBy("balance")
)
