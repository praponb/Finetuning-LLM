from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("s.city").isin(["Singapore", "Berlin"]), "core").otherwise("other").alias("city_group"),
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
    )
    .agg(
        F.count(F.when(F.col("o.status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sum(F.when(F.col("o.status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .orderBy("city_group", "opened_quarter")
)
