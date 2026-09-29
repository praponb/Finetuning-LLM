from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("s.monthly_fee") > 210) & ~F.col("s.plan").isin(["enterprise", "standard", "premium"]))
    .select(F.col("s.id"), F.col("s.start_date"), F.col("c.email"))
    .orderBy(F.col("email").desc(), F.col("id").desc())
)
