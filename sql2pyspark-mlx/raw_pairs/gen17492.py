from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.monthly_fee"),
        F.col("s.status"),
        F.col("s.plan"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.col("c.age"),
        F.concat(F.col("c.name"), F.lit(" "), F.col("c.segment")).alias("name_segment"),
    )
)
