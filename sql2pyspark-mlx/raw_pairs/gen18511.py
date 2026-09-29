from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("store_id") == 40).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.city").isin(["London", "Chiang Mai", "Tokyo"]))
    .select(F.col("c.id"), F.col("c.age"), F.col("c.is_active"), F.col("c.name"))
    .orderBy("id")
)
