from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.city").isin(["New York", "Berlin"]))
    .select(
        F.col("s.plan"),
        F.col("s.monthly_fee"),
        F.col("c.age"),
        F.concat(F.col("s.status"), F.lit("/"), F.col("s.plan")).alias("status_plan"),
    )
    .orderBy(F.col("age").desc())
)
