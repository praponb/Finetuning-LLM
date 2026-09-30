from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("c.segment"),
        F.col("c.is_active"),
        F.col("c.age"),
        F.col("s.plan"),
        F.col("s.end_date"),
        F.col("s.customer_id"),
        F.initcap(F.trim(F.col("c.name"))).alias("name_title"),
    )
    .orderBy("name_title")
)
