from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.upper(F.col("s.store_name")).like("%OWN%"))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("c.city").alias("customer_city"),
        F.when(F.col("s.region").isin(["Central", "West"]), "Core").otherwise("Other").alias("region_group"),
    )
    .orderBy("city", "region_group")
)
