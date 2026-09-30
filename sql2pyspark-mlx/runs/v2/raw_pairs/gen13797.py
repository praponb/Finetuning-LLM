from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .filter(F.col("o.customer_id").isNotNull())
    .join(df_customers.filter(F.col("is_active")).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("o.status") != "CANCELLED")
    .select(F.col("o.id"), F.col("o.status"), F.col("o.store_id"))
    .orderBy(F.col("store_id").desc(), "status", "id")
    .limit(5)
)
