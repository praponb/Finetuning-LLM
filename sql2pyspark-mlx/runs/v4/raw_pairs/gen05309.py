from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(~F.col("segment").isin(["SMB", "Consumer"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.month(F.col("s.end_date")).isin([3, 5, 6]))
    .select(F.col("s.id"), F.col("s.status"))
)
