from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(~F.col("s.plan").isin(["premium", "enterprise"]))
    .select(
        F.col("c.name"),
        F.col("s.end_date"),
        F.add_months(F.col("s.start_date"), 3).alias("start_date_plus_months"),
    )
)
