from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country").isin(["SG", "UK"]) & F.col("c.email").isNotNull())
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("c.age"),
        F.col("c.city"),
        F.regexp_replace(F.col("c.email"), " ", "").alias("email_slug"),
    )
    .orderBy(F.col("age").desc(), F.col("id").desc())
    .limit(10)
)
