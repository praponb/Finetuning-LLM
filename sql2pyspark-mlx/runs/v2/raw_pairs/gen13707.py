from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.country").isin(["SG", "UK", "TH"]))
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.country"))
)
