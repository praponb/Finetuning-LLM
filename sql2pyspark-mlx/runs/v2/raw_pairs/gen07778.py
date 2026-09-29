from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.customer_id").isNotNull())
    .join(df_customers.filter(F.col("name").like("%Emm%")).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.discount"))
)
