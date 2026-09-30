from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter((F.col("s.monthly_fee") > 130) | (F.col("s.start_date") <= "2024-01-25"))
    .select(
        F.col("s.monthly_fee"),
        F.col("c.country"),
        F.col("c.age"),
        F.concat(F.col("s.status"), F.lit(" - "), F.col("s.plan")).alias("status_plan"),
    )
    .orderBy("status_plan")
)
