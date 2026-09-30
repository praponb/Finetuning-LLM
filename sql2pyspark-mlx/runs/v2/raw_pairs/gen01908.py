from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .filter(F.col("s.customer_id").isNotNull())
    .join(df_customers.filter(F.upper(F.trim("email")) == "IVY@GMAIL.COM").alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.month(F.col("s.start_date")).isin([4, 8, 10]))
    .select(F.col("s.id"), F.col("s.monthly_fee"))
)
