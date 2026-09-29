from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.name").like("%Mia%"))
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("c.country"),
        F.date_trunc("MONTH", F.col("s.end_date")).alias("end_date_period"),
    )
    .orderBy(F.col("id").desc(), "end_date_period")
)
