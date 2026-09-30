from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.customer_id") == 38)
    .groupBy(
        F.when(F.col("c.country").isin(["JP", "UK"]), "core").otherwise("other").alias("country_group"),
        F.col("c.segment"),
    )
    .agg(
        F.max(F.col("s.shipped_date")).alias("latest_shipped_date"),
        F.min(F.col("s.delivered_date")).alias("first_delivered_date"),
    )
    .orderBy("latest_shipped_date", F.col("country_group").desc())
)
