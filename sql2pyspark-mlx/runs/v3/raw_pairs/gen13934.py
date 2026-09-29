from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.status") != "PENDING")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.count("*").alias("num_orders"))
)
