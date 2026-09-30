from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.customer_id").isNotNull())
    .join(df_customers.filter(~F.col("country").isin(["US", "TH"])).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull())
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("o.amount"))
)
