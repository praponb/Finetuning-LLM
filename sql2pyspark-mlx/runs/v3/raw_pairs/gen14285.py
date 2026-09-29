from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("monthly_fee") <= 80).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(~F.col("c.segment").isin(["Consumer", "SMB", "Corporate"]))
    .select(F.col("c.id"), F.col("c.is_active"))
)
