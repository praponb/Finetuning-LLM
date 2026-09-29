from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.credit_limit").between(4500, 15700))
    .select(
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("a.balance"),
        F.initcap(F.trim(F.col("c.city"))).alias("city_title"),
    )
    .orderBy(F.col("age").desc())
)
