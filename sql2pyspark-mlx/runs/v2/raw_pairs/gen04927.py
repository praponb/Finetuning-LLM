from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active"))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.age"),
    )
    .orderBy(F.col("age").desc(), F.col("customer_id").desc())
)
