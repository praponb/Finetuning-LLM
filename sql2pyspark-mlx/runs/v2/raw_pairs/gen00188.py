from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(F.countDistinct(F.col("o.shipping_fee")).alias("unique_channels"))
    .filter(F.col("unique_channels") > 4)
)
