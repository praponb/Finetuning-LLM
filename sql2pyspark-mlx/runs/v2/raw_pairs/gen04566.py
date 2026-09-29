from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(~F.col("c.name").like("He%"))
    .select(
        F.col("c.country"),
        F.col("c.age"),
        F.col("c.name"),
        F.col("a.balance"),
        F.col("a.status"),
    )
    .orderBy(F.col("age").desc())
)
